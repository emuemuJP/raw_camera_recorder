#include "raw_camera_recorder/raw_camera_recorder.hpp"
#include "rclcpp/rclcpp.hpp"

int main(int argc, char ** argv)
{
  setvbuf(stdout, nullptr, _IONBF, BUFSIZ);
  rclcpp::init(argc, argv);
  rclcpp::NodeOptions options;
  auto node = std::make_shared<raw_camera_recorder::RawCameraRecorder>(options);
  auto result = rcutils_logging_set_logger_level(
    node->get_logger().get_name(), RCUTILS_LOG_SEVERITY_INFO);
  (void)result;
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}
