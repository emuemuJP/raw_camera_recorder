#include "raw_camera_recorder/raw_camera_recorder.hpp"

extern "C" {
#include "gst/gst.h"
}

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace raw_camera_recorder
{

struct CameraPipeline
{
  int cam_id;
  std::string device;
  GstElement * pipeline = nullptr;
  int64_t time_offset = 0;  // signed to avoid unsigned underflow

  // Software gate: CSV recording starts only when recording==true.
  // Pre-gate frames are dropped via GST_PAD_PROBE_DROP so they never
  // reach splitmuxsink — this saves SSD bandwidth during the settle period.
  std::atomic<bool> recording{false};

  // Bresenham-style frame rate reduction (replaces GStreamer videorate element,
  // which caused one random camera to starve under 6-camera load).
  int capture_fps = 30;
  int record_fps = 20;
  int fps_accum = 0;

  // Timestamp recording (written from pad probe in GStreamer streaming thread)
  std::ofstream csv_file;
  uint64_t frame_index = 0;   // CSV frame index (starts from 0 at gate open)
  uint64_t skipped_frames = 0; // pre-gate frames dropped (for logging)
  uint64_t fps_dropped = 0;    // frames dropped by fps reduction (for logging)
};

// Pad probe callback: handles frame dropping and CSV timestamp writing.
// Runs in GStreamer's streaming thread — no tee or appsink needed.
// Placed on the recording queue's src pad.  Returning GST_PAD_PROBE_DROP
// prevents the buffer from reaching splitmuxsink, so MKV and CSV always match.
static GstPadProbeReturn timestamp_probe_cb(
  GstPad * /*pad*/, GstPadProbeInfo * info, gpointer user_data)
{
  auto * cam = static_cast<CameraPipeline *>(user_data);
  GstBuffer * buf = GST_PAD_PROBE_INFO_BUFFER(info);
  if (!buf) {
    return GST_PAD_PROBE_OK;
  }

  // Pre-gate: let frames through to splitmuxsink (MKV) but don't write CSV.
  // We must NOT drop pre-gate frames because splitmuxsink needs buffers
  // to complete its PAUSED→PLAYING transition.
  if (!cam->recording.load(std::memory_order_relaxed)) {
    cam->skipped_frames++;
    return GST_PAD_PROBE_OK;
  }

  // Bresenham-style frame rate reduction (replaces videorate element).
  // For 30→20fps: keeps 2 out of every 3 frames.
  if (cam->record_fps < cam->capture_fps) {
    cam->fps_accum += cam->record_fps;
    if (cam->fps_accum < cam->capture_fps) {
      cam->fps_dropped++;
      return GST_PAD_PROBE_DROP;
    }
    cam->fps_accum -= cam->capture_fps;
  }

  // Compute ROS2 timestamp (same method as gscam_h264)
  int64_t gst_pts_ns = 0;
  int64_t ros2_time_ns = 0;

  if (GST_BUFFER_PTS_IS_VALID(buf)) {
    gst_pts_ns = static_cast<int64_t>(buf->pts);
    GstClockTime bt = gst_element_get_base_time(cam->pipeline);
    ros2_time_ns = static_cast<int64_t>(buf->pts + bt) + cam->time_offset;
    if (ros2_time_ns < 0) {
      ros2_time_ns = 0;
    }
  } else {
    gst_pts_ns = -1;
  }

  cam->csv_file << cam->frame_index << ","
                << gst_pts_ns << ","
                << ros2_time_ns << "\n";

  // Periodic flush every 20 frames (~1s at 20fps)
  if (cam->frame_index % 20 == 0) {
    cam->csv_file.flush();
  }

  cam->frame_index++;
  return GST_PAD_PROBE_OK;
}

static std::string timestamp_string()
{
  auto now = std::chrono::system_clock::now();
  auto t = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&t, &tm);
  std::ostringstream ss;
  ss << std::put_time(&tm, "%Y%m%d%H%M");
  return ss.str();
}

class RawCameraRecorder::Impl
{
public:
  explicit Impl(rclcpp::Node * node);
  ~Impl();

private:
  rclcpp::Node * node_;
  std::vector<std::unique_ptr<CameraPipeline>> cameras_;
  std::thread duration_thread_;

  // Parameters
  int camera_count_;
  std::string device_base_;
  int device_index_;
  int width_;
  int height_;
  int capture_fps_;
  int record_fps_;
  std::string pixel_format_;
  std::string output_dir_;
  std::string session_dir_;
  int64_t segment_duration_ns_;
  int duration_sec_;
  int io_mode_;

  std::string ts_string_;

  std::string build_pipeline_string(const CameraPipeline & cam) const;
  bool create_and_pause(CameraPipeline & cam);
  bool set_playing(CameraPipeline & cam);
  void stop_camera(CameraPipeline & cam);
  void log_bus_messages(CameraPipeline & cam);
  void cleanup_failed_pipeline(CameraPipeline & cam);
};

RawCameraRecorder::Impl::Impl(rclcpp::Node * node)
: node_(node)
{
  camera_count_ = node_->declare_parameter("camera_count", 6);
  device_base_ = node_->declare_parameter("device_base", std::string("/dev/video"));
  device_index_ = node_->declare_parameter("device_index", 0);
  width_ = node_->declare_parameter("width", 1920);
  height_ = node_->declare_parameter("height", 1280);
  capture_fps_ = node_->declare_parameter("capture_fps", 30);
  record_fps_ = node_->declare_parameter("record_fps", 20);
  pixel_format_ = node_->declare_parameter("pixel_format", std::string("UYVY"));
  output_dir_ = node_->declare_parameter("output_dir", std::string("/mnt/ssd/raw_record"));
  segment_duration_ns_ = node_->declare_parameter("segment_duration_ns",
    static_cast<int64_t>(10000000000LL));
  duration_sec_ = node_->declare_parameter("duration_sec", 0);
  io_mode_ = node_->declare_parameter("io_mode", 2);

  ts_string_ = timestamp_string();
  session_dir_ = output_dir_ + "/" + ts_string_;

  RCLCPP_INFO(node_->get_logger(),
    "RawCameraRecorder: cameras=%d, %dx%d, capture=%dfps, record=%dfps, "
    "format=%s, output=%s, duration=%ds, io_mode=%d",
    camera_count_, width_, height_, capture_fps_, record_fps_,
    pixel_format_.c_str(), session_dir_.c_str(), duration_sec_, io_mode_);

  if (!gst_is_initialized()) {
    gst_init(nullptr, nullptr);
    RCLCPP_INFO(node_->get_logger(), "GStreamer initialized: %s", gst_version_string());
  }

  // Ensure session directory exists
  std::string mkdir_cmd = "mkdir -p " + session_dir_;
  int ret = system(mkdir_cmd.c_str());
  (void)ret;

  // Build camera specs
  cameras_.reserve(camera_count_);

  // ---- Phase 1: create all pipelines, attach pad probes, reach PAUSED ----
  for (int i = 0; i < camera_count_; ++i) {
    int id = device_index_ + i;
    auto cam = std::make_unique<CameraPipeline>();
    cam->cam_id = id;
    cam->device = device_base_ + std::to_string(id);
    cam->capture_fps = capture_fps_;
    cam->record_fps = record_fps_;

    RCLCPP_INFO(node_->get_logger(), "[Phase1] camera %d: device=%s",
      cam->cam_id, cam->device.c_str());

    if (create_and_pause(*cam)) {
      RCLCPP_INFO(node_->get_logger(), "[Phase1] camera %d: PAUSED ok", cam->cam_id);
      cameras_.push_back(std::move(cam));
    } else {
      RCLCPP_ERROR(node_->get_logger(),
        "[Phase1] camera %d (%s): failed, skipping",
        cam->cam_id, cam->device.c_str());
    }

    // Serialise Tegra driver init
    if (i + 1 < camera_count_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  RCLCPP_INFO(node_->get_logger(), "[Phase1] %zu / %d pipelines paused",
    cameras_.size(), camera_count_);

  // ---- Phase 2: transition to PLAYING sequentially ----
  // Sequential with delays is more reliable than simultaneous on Tegra.
  size_t playing_count = 0;
  for (size_t i = 0; i < cameras_.size(); ++i) {
    if (set_playing(*cameras_[i])) {
      ++playing_count;
      RCLCPP_INFO(node_->get_logger(), "[Phase2] camera %d: PLAYING",
        cameras_[i]->cam_id);
    } else {
      RCLCPP_ERROR(node_->get_logger(), "[Phase2] camera %d: failed to reach PLAYING",
        cameras_[i]->cam_id);
    }
    if (i + 1 < cameras_.size()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  RCLCPP_INFO(node_->get_logger(), "Startup complete: %zu / %d cameras PLAYING",
    playing_count, camera_count_);

  // ---- Phase 3: wait for pipelines to settle, then start CSV recording ----
  RCLCPP_INFO(node_->get_logger(),
    "[Phase3] Waiting 3s for all pipelines to settle...");
  std::this_thread::sleep_for(std::chrono::seconds(3));
  RCLCPP_INFO(node_->get_logger(), "[Phase3] Starting synchronized CSV recording");
  for (auto & cam : cameras_) {
    cam->recording = true;
  }
  RCLCPP_INFO(node_->get_logger(), "[Phase3] All %zu cameras recording", cameras_.size());

  // ---- Duration timer ----
  // Send EOS to all pipelines so splitmuxsink finalizes MKV files.
  // Pad probe will see all remaining buffers before EOS stops the pipeline.
  if (duration_sec_ > 0) {
    duration_thread_ = std::thread([this]() {
      std::this_thread::sleep_for(std::chrono::seconds(duration_sec_));
      RCLCPP_INFO(node_->get_logger(), "Duration %ds reached, sending EOS...", duration_sec_);
      for (auto & cam : cameras_) {
        if (cam->pipeline) {
          gst_element_send_event(cam->pipeline, gst_event_new_eos());
        }
      }
      // Wait for EOS to propagate through all pipelines
      std::this_thread::sleep_for(std::chrono::seconds(3));
      rclcpp::shutdown();
    });
    duration_thread_.detach();
  }
}

RawCameraRecorder::Impl::~Impl()
{
  for (auto & cam : cameras_) {
    stop_camera(*cam);
  }
  cameras_.clear();
}

std::string RawCameraRecorder::Impl::build_pipeline_string(
  const CameraPipeline & cam) const
{
  std::ostringstream ss;

  // Source
  ss << "v4l2src device=" << cam.device
     << " io-mode=" << io_mode_
     << " do-timestamp=true"
     << " ! video/x-raw,format=UYVY"
     << ",width=" << width_
     << ",height=" << height_
     << ",framerate=" << capture_fps_ << "/1";

  // Optional I420 conversion
  if (pixel_format_ == "I420") {
    ss << " ! nvvidconv"
       << " ! video/x-raw,format=I420"
       << ",width=" << width_
       << ",height=" << height_;
  }

  // Frame rate reduction is handled by the pad probe callback
  // (Bresenham-style frame dropping), NOT by the videorate element.
  // videorate caused one random camera to starve under 6-camera RAW load.

  // Single recording queue → splitmuxsink.
  // A pad probe on this queue's src pad captures timestamps for CSV,
  // eliminating the need for a tee + appsink branch entirely.
  std::string mkv_pattern = session_dir_ + "/cam" + std::to_string(cam.cam_id)
    + "_raw_" + ts_string_ + "_%05d.mkv";
  ss << " ! queue name=recq max-size-buffers=60"
     << " max-size-time=0 max-size-bytes=0"
     << " ! splitmuxsink name=mux muxer=matroskamux"
     << " location=" << mkv_pattern
     << " max-size-time=" << segment_duration_ns_
     << " sync=false";

  return ss.str();
}

void RawCameraRecorder::Impl::log_bus_messages(CameraPipeline & cam)
{
  GstBus * bus = gst_element_get_bus(cam.pipeline);
  if (!bus) {
    return;
  }
  while (true) {
    GstMessage * msg = gst_bus_pop_filtered(bus,
      static_cast<GstMessageType>(GST_MESSAGE_ERROR | GST_MESSAGE_WARNING));
    if (!msg) {
      break;
    }
    GError * err = nullptr;
    gchar * debug_info = nullptr;
    if (GST_MESSAGE_TYPE(msg) == GST_MESSAGE_ERROR) {
      gst_message_parse_error(msg, &err, &debug_info);
      RCLCPP_ERROR(node_->get_logger(), "Camera %d GstError: %s\n  debug: %s",
        cam.cam_id, err->message, debug_info ? debug_info : "(none)");
    } else {
      gst_message_parse_warning(msg, &err, &debug_info);
      RCLCPP_WARN(node_->get_logger(), "Camera %d GstWarning: %s\n  debug: %s",
        cam.cam_id, err->message, debug_info ? debug_info : "(none)");
    }
    g_free(debug_info);
    g_error_free(err);
    gst_message_unref(msg);
  }
  gst_object_unref(bus);
}

void RawCameraRecorder::Impl::cleanup_failed_pipeline(CameraPipeline & cam)
{
  log_bus_messages(cam);
  if (cam.pipeline) {
    gst_element_set_state(cam.pipeline, GST_STATE_NULL);
    gst_object_unref(cam.pipeline);
    cam.pipeline = nullptr;
  }
}

bool RawCameraRecorder::Impl::create_and_pause(CameraPipeline & cam)
{
  std::string pipeline_str = build_pipeline_string(cam);
  RCLCPP_INFO(node_->get_logger(), "Camera %d pipeline:\n  %s",
    cam.cam_id, pipeline_str.c_str());

  GError * error = nullptr;
  cam.pipeline = gst_parse_launch(pipeline_str.c_str(), &error);
  if (!cam.pipeline) {
    RCLCPP_ERROR(node_->get_logger(), "Camera %d: failed to create pipeline: %s",
      cam.cam_id, error ? error->message : "unknown");
    if (error) {
      g_error_free(error);
    }
    return false;
  }
  if (error) {
    RCLCPP_WARN(node_->get_logger(), "Camera %d: pipeline parse warning: %s",
      cam.cam_id, error->message);
    g_error_free(error);
  }

  // Attach pad probe on recording queue's src pad for CSV timestamps.
  // Every buffer flowing to splitmuxsink passes through this probe.
  GstElement * recq = gst_bin_get_by_name(GST_BIN(cam.pipeline), "recq");
  if (!recq) {
    RCLCPP_ERROR(node_->get_logger(), "Camera %d: queue 'recq' not found", cam.cam_id);
    gst_object_unref(cam.pipeline);
    cam.pipeline = nullptr;
    return false;
  }
  GstPad * srcpad = gst_element_get_static_pad(recq, "src");
  if (!srcpad) {
    RCLCPP_ERROR(node_->get_logger(), "Camera %d: queue src pad not found", cam.cam_id);
    gst_object_unref(recq);
    gst_object_unref(cam.pipeline);
    cam.pipeline = nullptr;
    return false;
  }
  gst_pad_add_probe(srcpad,
    GST_PAD_PROBE_TYPE_BUFFER, timestamp_probe_cb, &cam, nullptr);
  gst_object_unref(srcpad);
  gst_object_unref(recq);

  // Calibrate GStreamer clock -> ROS time (signed to avoid underflow)
  GstClock * clock = gst_system_clock_obtain();
  GstClockTime ct = gst_clock_get_time(clock);
  gst_object_unref(clock);
  cam.time_offset = node_->now().nanoseconds() - static_cast<int64_t>(ct);

  // Open CSV file for timestamps
  std::string csv_path = session_dir_ + "/cam" + std::to_string(cam.cam_id)
    + "_timestamps_" + ts_string_ + ".csv";
  cam.csv_file.open(csv_path);
  if (!cam.csv_file.is_open()) {
    RCLCPP_ERROR(node_->get_logger(), "Camera %d: failed to open CSV: %s",
      cam.cam_id, csv_path.c_str());
    cleanup_failed_pipeline(cam);
    return false;
  }
  cam.csv_file << "frame_index,gst_pts_ns,ros2_time_ns\n";
  RCLCPP_INFO(node_->get_logger(), "Camera %d: CSV -> %s",
    cam.cam_id, csv_path.c_str());

  // NULL -> PAUSED only (no stream flows yet)
  RCLCPP_INFO(node_->get_logger(), "Camera %d: -> PAUSED ...", cam.cam_id);
  GstStateChangeReturn ret = gst_element_set_state(cam.pipeline, GST_STATE_PAUSED);
  if (ret == GST_STATE_CHANGE_FAILURE) {
    RCLCPP_ERROR(node_->get_logger(),
      "Camera %d: set_state PAUSED returned FAILURE", cam.cam_id);
    cleanup_failed_pipeline(cam);
    return false;
  }
  ret = gst_element_get_state(cam.pipeline, nullptr, nullptr, 10 * GST_SECOND);
  if (ret == GST_STATE_CHANGE_FAILURE) {
    RCLCPP_ERROR(node_->get_logger(),
      "Camera %d: failed to reach PAUSED", cam.cam_id);
    cleanup_failed_pipeline(cam);
    return false;
  }

  return true;
}

bool RawCameraRecorder::Impl::set_playing(CameraPipeline & cam)
{
  RCLCPP_INFO(node_->get_logger(), "Camera %d: -> PLAYING ...", cam.cam_id);
  GstStateChangeReturn ret = gst_element_set_state(cam.pipeline, GST_STATE_PLAYING);
  if (ret == GST_STATE_CHANGE_FAILURE) {
    RCLCPP_ERROR(node_->get_logger(),
      "Camera %d: set_state PLAYING returned FAILURE", cam.cam_id);
    log_bus_messages(cam);
    return false;
  }
  // Wait for PLAYING to complete so the pipeline is fully streaming
  ret = gst_element_get_state(cam.pipeline, nullptr, nullptr, 10 * GST_SECOND);
  if (ret == GST_STATE_CHANGE_FAILURE) {
    RCLCPP_ERROR(node_->get_logger(),
      "Camera %d: failed to reach PLAYING", cam.cam_id);
    log_bus_messages(cam);
    return false;
  }
  return true;
}

void RawCameraRecorder::Impl::stop_camera(CameraPipeline & cam)
{
  // Send EOS so splitmuxsink finalizes MKV files
  if (cam.pipeline) {
    gst_element_send_event(cam.pipeline, gst_event_new_eos());
  }

  // Set to NULL — this stops streaming threads and ensures the pad probe
  // callback will not fire after this point.
  if (cam.pipeline) {
    gst_element_set_state(cam.pipeline, GST_STATE_NULL);
    gst_object_unref(cam.pipeline);
    cam.pipeline = nullptr;
  }

  // Close CSV file (safe: pad probe no longer runs after set_state NULL)
  if (cam.csv_file.is_open()) {
    cam.csv_file.flush();
    cam.csv_file.close();
  }

  RCLCPP_INFO(node_->get_logger(),
    "Camera %d stopped (%lu CSV frames, %lu pre-gate skipped, %lu fps-dropped)",
    cam.cam_id, cam.frame_index, cam.skipped_frames, cam.fps_dropped);
}

// ---- Public interface ----

RawCameraRecorder::RawCameraRecorder(const rclcpp::NodeOptions & options)
: rclcpp::Node("raw_camera_recorder", options),
  impl_(std::make_unique<Impl>(this))
{
}

RawCameraRecorder::~RawCameraRecorder()
{
  impl_.reset();
}

}  // namespace raw_camera_recorder
