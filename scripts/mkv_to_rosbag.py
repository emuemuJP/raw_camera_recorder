#!/usr/bin/env python3
"""Convert raw MKV recordings + timestamp CSVs to ROS2 MCAP bag.

Usage:
    # Raw BGR (default, ~4GB per camera):
    python3 mkv_to_rosbag.py /mnt/ssd/raw_record -o /mnt/ssd/raw_record/bag

    # Raw BGR, single combined file:
    python3 mkv_to_rosbag.py /mnt/ssd/raw_record -o /mnt/ssd/raw_record/bag --single

    # JPEG compressed (~200MB per camera):
    python3 mkv_to_rosbag.py /mnt/ssd/raw_record -o /mnt/ssd/raw_record/bag --compress

The script expects the following files in the input directory:
    cam{id}_raw_{TS}_{seg:05d}.mkv   - MKV video segments
    cam{id}_timestamps_{TS}.csv      - Per-frame timestamp CSV

MKV files contain pre-gate frames (30fps) followed by recording frames (20fps).
The CSV only covers recording frames.  This script counts MKV frames, skips
the pre-gate portion, then maps remaining MKV frames 1:1 to CSV rows.

Optimizations:
  - ffmpeg subprocess for MKV reading + color conversion (C/SIMD speed)
  - JPEG mode (default): ffmpeg MJPEG encoding, ~20x smaller output
  - multiprocessing for parallel camera processing (one process per camera)
  - ffprobe for fast frame counting (no decode)

Output is a ROS2 bag directory with metadata.yaml + per-camera MCAP files.
Does NOT require a running ROS2 system (standalone).
"""

import argparse
import csv
import glob
import os
import re
import subprocess
import sys
import time
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed

from mcap.reader import make_reader
from mcap_ros2.writer import Writer as McapWriter
from sensor_msgs.msg import Image, CompressedImage


IMAGE_MSGDEF = """\
std_msgs/Header header
uint32 height
uint32 width
string encoding
uint8 is_bigendian
uint32 step
uint8[] data
===
MSG: std_msgs/msg/Header
builtin_interfaces/Time stamp
string frame_id"""

COMPRESSED_IMAGE_MSGDEF = """\
std_msgs/Header header
string format
uint8[] data
===
MSG: std_msgs/msg/Header
builtin_interfaces/Time stamp
string frame_id"""


def load_timestamps(csv_path):
    """Load CSV -> list of {frame_index, gst_pts_ns, ros2_time_ns} dicts."""
    entries = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            entries.append({
                'frame_index': int(row['frame_index']),
                'gst_pts_ns': int(row['gst_pts_ns']),
                'ros2_time_ns': int(row['ros2_time_ns']),
            })
    return entries


def discover_cameras(input_dir):
    """Discover camera IDs and their MKV + CSV file groups.

    Returns dict: cam_id -> {
        'mkv_files': [sorted list of MKV segment paths],
        'csv_file': path to timestamp CSV,
        'timestamp': recording timestamp string
    }
    """
    cameras = {}

    mkv_pattern = os.path.join(input_dir, 'cam*_raw_*_*.mkv')
    mkv_files = sorted(glob.glob(mkv_pattern))

    for mkv_path in mkv_files:
        basename = os.path.basename(mkv_path)
        match = re.match(r'cam(\d+)_raw_(\d+)_(\d+)\.mkv', basename)
        if not match:
            continue
        cam_id = int(match.group(1))
        ts = match.group(2)

        if cam_id not in cameras:
            cameras[cam_id] = {
                'mkv_files': [],
                'timestamp': ts,
            }
        cameras[cam_id]['mkv_files'].append(mkv_path)

    for cam_id in cameras:
        cameras[cam_id]['mkv_files'].sort()
        ts = cameras[cam_id]['timestamp']
        csv_path = os.path.join(input_dir, f'cam{cam_id}_timestamps_{ts}.csv')
        if os.path.exists(csv_path):
            cameras[cam_id]['csv_file'] = csv_path
        else:
            print(f'WARNING: No CSV found for cam{cam_id}: {csv_path}')
            cameras[cam_id]['csv_file'] = None

    return cameras


def count_mkv_frames_ffprobe(mkv_files):
    """Count total frames across MKV segments using ffprobe (no decode)."""
    total = 0
    for mkv_path in mkv_files:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-count_packets',
             '-select_streams', 'v:0',
             '-show_entries', 'stream=nb_read_packets',
             '-of', 'csv=p=0', mkv_path],
            capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            total += int(result.stdout.strip())
    return total


def make_concat_file(mkv_files, concat_path):
    """Write ffmpeg concat demuxer file."""
    with open(concat_path, 'w') as f:
        for mkv_path in mkv_files:
            f.write(f"file '{mkv_path}'\n")


def read_jpeg_frames(pipe):
    """Read concatenated JPEG frames from an image2pipe stream.

    JPEG SOI = 0xFF 0xD8, EOI = 0xFF 0xD9.
    In JPEG entropy data 0xFF is always followed by 0x00 (byte stuffing),
    so SOI/EOI markers unambiguously delimit frame boundaries.
    """
    buf = b''
    CHUNK = 256 * 1024  # 256KB read chunks
    while True:
        chunk = pipe.read(CHUNK)
        if not chunk:
            break
        buf += chunk
        # Yield complete JPEG frames (SOI to EOI)
        while True:
            eoi = buf.find(b'\xff\xd9')
            if eoi == -1:
                break
            frame = buf[:eoi + 2]
            buf = buf[eoi + 2:]
            if frame[:2] == b'\xff\xd8':
                yield frame


def process_camera_jpeg(cam_id, mkv_files, csv_path, mcap_path,
                        width, height, jpeg_quality):
    """Process one camera: ffmpeg MJPEG pipe -> CompressedImage MCAP.

    Returns (cam_id, frames_written, file_size_mb, min_time_ns, max_time_ns).
    """
    ts_entries = load_timestamps(csv_path)
    total_csv = len(ts_entries)
    topic = f'/cam{cam_id}/image_raw/compressed'
    frame_id = f'cam{cam_id}_frame'

    # Count MKV frames to determine pre-gate skip
    total_mkv = count_mkv_frames_ffprobe(mkv_files)
    skip_count = max(0, total_mkv - total_csv)
    print(f'  cam{cam_id}: MKV={total_mkv} CSV={total_csv} skip={skip_count}',
          flush=True)

    # Build ffmpeg concat input
    concat_path = mcap_path + '.concat.txt'
    make_concat_file(mkv_files, concat_path)

    # ffmpeg: concat -> skip pre-gate -> MJPEG pipe
    # select filter skips first skip_count frames without encoding them
    vf = f"select='gte(n\\,{skip_count})',setpts=N/TB" if skip_count > 0 else ''
    cmd = ['ffmpeg', '-v', 'error',
           '-f', 'concat', '-safe', '0', '-i', concat_path]
    if vf:
        cmd += ['-vf', vf]
    cmd += ['-codec:v', 'mjpeg', '-q:v', str(jpeg_quality),
            '-f', 'image2pipe', 'pipe:1']

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, bufsize=1024 * 1024)

    frames_written = 0
    min_time = max_time = 0

    try:
        with open(mcap_path, 'wb') as f:
            writer = McapWriter(f)
            schema = writer.register_msgdef(
                'sensor_msgs/msg/CompressedImage', COMPRESSED_IMAGE_MSGDEF)

            for jpeg_data in read_jpeg_frames(proc.stdout):
                if frames_written >= total_csv:
                    break

                entry = ts_entries[frames_written]
                ros2_ns = entry['ros2_time_ns']

                if frames_written == 0:
                    min_time = ros2_ns
                max_time = ros2_ns

                msg = CompressedImage()
                msg.header.stamp.sec = int(ros2_ns // 1_000_000_000)
                msg.header.stamp.nanosec = int(ros2_ns % 1_000_000_000)
                msg.header.frame_id = frame_id
                msg.format = 'jpeg'
                msg.data = jpeg_data

                writer.write_message(
                    topic=topic,
                    schema=schema,
                    message=msg,
                    log_time=ros2_ns,
                    publish_time=ros2_ns,
                    sequence=frames_written,
                )

                frames_written += 1
                if frames_written % 500 == 0:
                    print(f'    cam{cam_id}: {frames_written}/{total_csv}',
                          flush=True)

            writer.finish()

    finally:
        proc.stdout.close()
        proc.wait()
        try:
            os.unlink(concat_path)
        except OSError:
            pass

    file_size_mb = os.path.getsize(mcap_path) / (1024 * 1024)
    print(f'  cam{cam_id}: {frames_written} frames -> '
          f'{os.path.basename(mcap_path)} ({file_size_mb:.1f} MB)', flush=True)
    return cam_id, frames_written, file_size_mb, min_time, max_time


def process_camera_raw(cam_id, mkv_files, csv_path, mcap_path, width, height):
    """Process one camera: ffmpeg raw BGR pipe -> Image MCAP.

    Returns (cam_id, frames_written, file_size_mb, min_time_ns, max_time_ns).
    """
    ts_entries = load_timestamps(csv_path)
    total_csv = len(ts_entries)
    topic = f'/cam{cam_id}/image_raw'
    frame_id = f'cam{cam_id}_frame'
    frame_size = width * height * 3  # bgr24

    # Count MKV frames to determine pre-gate skip
    total_mkv = count_mkv_frames_ffprobe(mkv_files)
    skip_count = max(0, total_mkv - total_csv)
    print(f'  cam{cam_id}: MKV={total_mkv} CSV={total_csv} skip={skip_count}',
          flush=True)

    # Build ffmpeg concat input
    concat_path = mcap_path + '.concat.txt'
    make_concat_file(mkv_files, concat_path)

    cmd = ['ffmpeg', '-v', 'error',
           '-f', 'concat', '-safe', '0', '-i', concat_path,
           '-f', 'rawvideo', '-pix_fmt', 'bgr24', 'pipe:1']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, bufsize=frame_size * 4)

    frames_written = 0
    min_time = max_time = 0

    try:
        # Skip pre-gate frames
        for _ in range(skip_count):
            data = proc.stdout.read(frame_size)
            if len(data) < frame_size:
                break

        with open(mcap_path, 'wb') as f:
            writer = McapWriter(f)
            schema = writer.register_msgdef('sensor_msgs/msg/Image',
                                            IMAGE_MSGDEF)

            for csv_idx in range(total_csv):
                data = proc.stdout.read(frame_size)
                if len(data) < frame_size:
                    print(f'  cam{cam_id}: EOF at frame {csv_idx}/{total_csv}',
                          flush=True)
                    break

                entry = ts_entries[csv_idx]
                ros2_ns = entry['ros2_time_ns']

                if frames_written == 0:
                    min_time = ros2_ns
                max_time = ros2_ns

                msg = Image()
                msg.header.stamp.sec = int(ros2_ns // 1_000_000_000)
                msg.header.stamp.nanosec = int(ros2_ns % 1_000_000_000)
                msg.header.frame_id = frame_id
                msg.height = height
                msg.width = width
                msg.encoding = 'bgr8'
                msg.is_bigendian = 0
                msg.step = width * 3
                msg.data = data

                writer.write_message(
                    topic=topic,
                    schema=schema,
                    message=msg,
                    log_time=ros2_ns,
                    publish_time=ros2_ns,
                    sequence=csv_idx,
                )

                frames_written += 1
                if frames_written % 500 == 0:
                    print(f'    cam{cam_id}: {frames_written}/{total_csv}',
                          flush=True)

            writer.finish()

    finally:
        proc.stdout.close()
        proc.wait()
        try:
            os.unlink(concat_path)
        except OSError:
            pass

    file_size_mb = os.path.getsize(mcap_path) / (1024 * 1024)
    print(f'  cam{cam_id}: {frames_written} frames -> '
          f'{os.path.basename(mcap_path)} ({file_size_mb:.1f} MB)', flush=True)
    return cam_id, frames_written, file_size_mb, min_time, max_time


def merge_mcap_files(input_paths, output_path):
    """Merge multiple MCAP files into a single file.

    Args:
        input_paths: List of MCAP file paths to merge.
        output_path: Path for the merged output file.
    """
    print(f'Merging {len(input_paths)} MCAP files...', flush=True)
    schema_cache = {}  # (name, data) -> registered schema

    with open(output_path, 'wb') as out_f:
        writer = McapWriter(out_f)

        for mcap_path in input_paths:
            print(f'  Reading {os.path.basename(mcap_path)}...', flush=True)
            with open(mcap_path, 'rb') as in_f:
                reader = make_reader(in_f)
                for schema, channel, message in reader.iter_messages():
                    # Register schema if not seen (cache by name + definition)
                    key = (schema.name, schema.data)
                    if key not in schema_cache:
                        schema_cache[key] = writer.register_msgdef(
                            schema.name, schema.data.decode())

                    # Write message with mapped schema
                    writer.write_message(
                        topic=channel.topic,
                        schema=schema_cache[key],
                        message=message.data,
                        log_time=message.log_time,
                        publish_time=message.publish_time,
                    )

        writer.finish()

    # Remove individual MCAP files
    for path in input_paths:
        os.unlink(path)
        print(f'  Removed {os.path.basename(path)}', flush=True)

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f'Merged: {os.path.basename(output_path)} ({file_size_mb:.1f} MB)',
          flush=True)


def generate_metadata_yaml(output_dir, mcap_files, topic_info, msg_type):
    """Generate metadata.yaml for ROS2 bag compatibility.

    topic_info: list of (topic_name, message_count, min_time_ns, max_time_ns)
    """
    all_min = min(t[2] for t in topic_info if t[2] > 0)
    all_max = max(t[3] for t in topic_info if t[3] > 0)
    total_messages = sum(t[1] for t in topic_info)
    duration_ns = all_max - all_min

    topics = []
    for topic_name, msg_count, _, _ in topic_info:
        topics.append({
            'topic_metadata': {
                'name': topic_name,
                'type': msg_type,
                'serialization_format': 'cdr',
                'offered_qos_profiles': '',
            },
            'message_count': msg_count,
        })

    relative_paths = [os.path.basename(p) for p in mcap_files]

    metadata = {
        'rosbag2_bagfile_information': {
            'version': 6,
            'storage_identifier': 'mcap',
            'relative_file_paths': relative_paths,
            'duration': {'nanoseconds': duration_ns},
            'starting_time': {'nanoseconds_since_epoch': all_min},
            'message_count': total_messages,
            'topics_with_message_count': topics,
            'compression_format': '',
            'compression_mode': '',
        }
    }

    yaml_path = os.path.join(output_dir, 'metadata.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)
    print(f'metadata.yaml -> {yaml_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Convert raw MKV + timestamp CSV to ROS2 MCAP bag')
    parser.add_argument('input_dir',
        help='Directory containing MKV and CSV files')
    parser.add_argument('--output', '-o', default=None,
        help='Output bag directory (default: {input_dir}/bag)')
    parser.add_argument('--cameras', '-c', default=None,
        help='Comma-separated camera IDs to process (default: all)')
    parser.add_argument('--width', type=int, default=1920)
    parser.add_argument('--height', type=int, default=1280)
    parser.add_argument('--compress', action='store_true',
        help='Use JPEG compressed (sensor_msgs/CompressedImage) instead of raw BGR')
    parser.add_argument('--raw', action='store_true',
        help='[DEPRECATED] Raw is now default, this flag is ignored')
    parser.add_argument('--jpeg-quality', type=int, default=2,
        help='JPEG quality for ffmpeg (2=best, 31=worst, default: 2)')
    parser.add_argument('--single', action='store_true',
        help='Output all cameras to a single MCAP file (combined.mcap)')
    parser.add_argument('--jobs', '-j', type=int, default=0,
        help='Parallel workers (default: number of cameras)')
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f'ERROR: {args.input_dir} is not a directory')
        sys.exit(1)

    output_dir = args.output or os.path.join(args.input_dir, 'bag')
    os.makedirs(output_dir, exist_ok=True)

    cameras = discover_cameras(args.input_dir)
    if not cameras:
        print('ERROR: No camera data found')
        sys.exit(1)

    if args.cameras:
        selected = set(int(x) for x in args.cameras.split(','))
        cameras = {k: v for k, v in cameras.items() if k in selected}

    valid_cameras = {k: v for k, v in cameras.items() if v['csv_file']}
    if not valid_cameras:
        print('ERROR: No cameras with valid CSV files')
        sys.exit(1)

    # Handle deprecated --raw flag
    if args.raw:
        print('Note: --raw is deprecated (raw is now default). Flag ignored.')

    # Determine compression mode (default: raw)
    use_compression = args.compress

    n_workers = args.jobs if args.jobs > 0 else len(valid_cameras)
    mode = f'jpeg (q={args.jpeg_quality})' if use_compression else 'raw (bgr8)'
    output_mode = 'single file' if args.single else 'per-camera files'
    print(f'Found {len(valid_cameras)} camera(s): {sorted(valid_cameras.keys())}')
    print(f'Output: {output_dir} ({output_mode})')
    print(f'Mode: {mode}')
    print(f'Workers: {n_workers}')
    print()

    t0 = time.time()
    results = []

    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = {}
        for cam_id in sorted(valid_cameras.keys()):
            cam = valid_cameras[cam_id]
            mcap_path = os.path.join(output_dir, f'cam{cam_id}.mcap')
            if use_compression:
                future = pool.submit(
                    process_camera_jpeg,
                    cam_id, cam['mkv_files'], cam['csv_file'],
                    mcap_path, args.width, args.height, args.jpeg_quality)
            else:
                future = pool.submit(
                    process_camera_raw,
                    cam_id, cam['mkv_files'], cam['csv_file'],
                    mcap_path, args.width, args.height)
            futures[future] = cam_id

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                cam_id = futures[future]
                print(f'  cam{cam_id}: ERROR: {e}')

    results.sort(key=lambda r: r[0])
    elapsed = time.time() - t0

    print()
    total_frames = sum(r[1] for r in results)
    total_mb = sum(r[2] for r in results)
    print(f'Total: {total_frames} frames, {total_mb:.1f} MB, {elapsed:.1f}s')

    # Determine message type and topic suffix
    if use_compression:
        msg_type = 'sensor_msgs/msg/CompressedImage'
        topic_suffix = '/image_raw/compressed'
    else:
        msg_type = 'sensor_msgs/msg/Image'
        topic_suffix = '/image_raw'

    topic_info = []
    mcap_files = []
    for cam_id, frames, mb, t_min, t_max in results:
        topic_info.append((f'/cam{cam_id}{topic_suffix}', frames, t_min, t_max))
        mcap_files.append(os.path.join(output_dir, f'cam{cam_id}.mcap'))

    # Merge MCAP files if --single mode
    if args.single and len(mcap_files) > 1:
        print()
        combined_path = os.path.join(output_dir, 'combined.mcap')
        merge_mcap_files(mcap_files, combined_path)
        mcap_files = [combined_path]

    if topic_info:
        generate_metadata_yaml(output_dir, mcap_files, topic_info, msg_type)

    print(f'Done: {output_dir}')


if __name__ == '__main__':
    main()
