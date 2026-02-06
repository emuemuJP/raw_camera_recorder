cmake_minimum_required(VERSION 3.6)

project(raw_camera_recorder)

if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 14)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# GStreamer doesn't provide CMake files
find_package(PkgConfig REQUIRED)
pkg_check_modules(GSTREAMER REQUIRED gstreamer-1.0 IMPORTED_TARGET)
pkg_check_modules(GST_APP REQUIRED gstreamer-app-1.0 IMPORTED_TARGET)

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)

include_directories(include)

#=============
# Raw Camera Recorder executable
#=============

add_executable(raw_camera_recorder_main
  src/raw_camera_recorder.cpp
  src/raw_camera_recorder_main.cpp
)

ament_target_dependencies(raw_camera_recorder_main
  rclcpp
)

target_link_libraries(raw_camera_recorder_main
  PkgConfig::GSTREAMER
  PkgConfig::GST_APP
)

target_include_directories(raw_camera_recorder_main PUBLIC
  "$<BUILD_INTERFACE:${GSTREAMER_INCLUDE_DIRS}>")

#=============
# Install
#=============

install(
  TARGETS raw_camera_recorder_main
  DESTINATION lib/${PROJECT_NAME}
)

install(DIRECTORY launch cfg DESTINATION share/${PROJECT_NAME})

install(PROGRAMS scripts/mkv_to_rosbag.py DESTINATION lib/${PROJECT_NAME})

ament_package()
