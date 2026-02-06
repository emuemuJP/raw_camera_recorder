#ifndef RAW_CAMERA_RECORDER__RAW_CAMERA_RECORDER_HPP_
#define RAW_CAMERA_RECORDER__RAW_CAMERA_RECORDER_HPP_

#include <memory>
#include "rclcpp/rclcpp.hpp"

namespace raw_camera_recorder
{

class RawCameraRecorder : public rclcpp::Node
{
public:
  explicit RawCameraRecorder(const rclcpp::NodeOptions & options);
  ~RawCameraRecorder() override;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace raw_camera_recorder

#endif  // RAW_CAMERA_RECORDER__RAW_CAMERA_RECORDER_HPP_
