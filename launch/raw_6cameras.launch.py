"""Launch raw_camera_recorder with 6 cameras."""

import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    pkg_dir = get_package_share_directory('raw_camera_recorder')
    default_params = os.path.join(pkg_dir, 'cfg', 'params.yaml')

    return LaunchDescription([
        DeclareLaunchArgument(
            'params_file',
            default_value=default_params,
            description='Path to parameters YAML file'
        ),
        DeclareLaunchArgument(
            'output_dir',
            default_value='/mnt/ssd/raw_record',
            description='Output directory for MKV and CSV files'
        ),
        DeclareLaunchArgument(
            'duration_sec',
            default_value='0',
            description='Recording duration in seconds (0 = unlimited)'
        ),
        Node(
            package='raw_camera_recorder',
            executable='raw_camera_recorder_main',
            output='screen',
            name='raw_camera_recorder',
            parameters=[
                LaunchConfiguration('params_file'),
                {
                    'output_dir': LaunchConfiguration('output_dir'),
                    'duration_sec': LaunchConfiguration('duration_sec'),
                },
            ],
        ),
    ])
