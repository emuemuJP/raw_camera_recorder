#!/usr/bin/env python3
"""Synchronized 6-camera playback tool.

Plays back raw MKV recordings from all cameras simultaneously,
synchronized by ROS2 timestamps from CSV files.

Usage:
    python3 play_synced.py /mnt/m2ssd/raw_record
    python3 play_synced.py /mnt/m2ssd/raw_record --speed 0.5
    python3 play_synced.py /mnt/m2ssd/raw_record --layout 2x3

Controls:
    Space       Play / Pause
    Right       Seek +5s
    Left        Seek -5s
    +/=         Speed up (0.25x steps)
    -           Speed down
    .           Step forward 1 frame (paused)
    ,           Step backward 1 frame (paused)
    q / Esc     Quit
"""

import argparse
import bisect
import collections
import csv
import glob
import os
import re
import subprocess
import sys
import threading
import time

import cv2
import numpy as np


# ---------------------------------------------------------------------------
# Reused from mkv_to_rosbag.py
# ---------------------------------------------------------------------------

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


def discover_sessions(input_dir):
    """Discover recording sessions (timestamps) in the directory.

    Supports both layouts:
      - Subfolder: input_dir/<timestamp>/cam*_raw_*.mkv
      - Flat:      input_dir/cam*_raw_*.mkv
    """
    sessions = {}  # ts -> directory containing the files

    # Check subdirectories first (new layout)
    for entry in sorted(os.listdir(input_dir)):
        subdir = os.path.join(input_dir, entry)
        if os.path.isdir(subdir) and re.match(r'^\d{12}$', entry):
            mkv_check = glob.glob(os.path.join(subdir, 'cam*_raw_*_*.mkv'))
            if mkv_check:
                sessions[entry] = subdir

    # Also check flat files in input_dir
    for mkv_path in glob.glob(os.path.join(input_dir, 'cam*_raw_*_*.mkv')):
        match = re.match(r'cam(\d+)_raw_(\d+)_(\d+)\.mkv',
                         os.path.basename(mkv_path))
        if match:
            ts = match.group(2)
            if ts not in sessions:
                sessions[ts] = input_dir

    return sessions


def discover_cameras(input_dir, session=None):
    """Discover camera IDs and their MKV + CSV file groups.

    If session is given, only load files for that timestamp session.
    If multiple sessions exist and none is specified, use the latest.
    """
    sessions = discover_sessions(input_dir)
    if not sessions:
        return {}

    sorted_ts = sorted(sessions.keys())

    if session:
        if session not in sessions:
            print(f'ERROR: Session {session} not found. '
                  f'Available: {sorted_ts}')
            return {}
        target_session = session
    else:
        if len(sorted_ts) > 1:
            print(f'Multiple sessions found: {sorted_ts}')
            print(f'Using latest: {sorted_ts[-1]} '
                  f'(use --session to select)')
        target_session = sorted_ts[-1]

    data_dir = sessions[target_session]
    print(f'Session {target_session}: {data_dir}')

    cameras = {}
    mkv_pattern = os.path.join(data_dir, 'cam*_raw_*_*.mkv')
    mkv_files = sorted(glob.glob(mkv_pattern))

    for mkv_path in mkv_files:
        basename = os.path.basename(mkv_path)
        match = re.match(r'cam(\d+)_raw_(\d+)_(\d+)\.mkv', basename)
        if not match:
            continue
        cam_id = int(match.group(1))
        ts = match.group(2)
        if ts != target_session:
            continue

        if cam_id not in cameras:
            cameras[cam_id] = {'mkv_files': [], 'timestamp': ts}
        cameras[cam_id]['mkv_files'].append(mkv_path)

    for cam_id in cameras:
        cameras[cam_id]['mkv_files'].sort()
        ts = cameras[cam_id]['timestamp']
        csv_path = os.path.join(data_dir, f'cam{cam_id}_timestamps_{ts}.csv')
        if os.path.exists(csv_path):
            cameras[cam_id]['csv_file'] = csv_path
        else:
            print(f'WARNING: No CSV found for cam{cam_id}: {csv_path}')
            cameras[cam_id]['csv_file'] = None

    return cameras


def _probe_duration(mkv_path):
    """Get segment duration in seconds via ffprobe (instant, header only).
    Falls back to count_packets for truncated segments."""
    result = subprocess.run(
        ['ffprobe', '-v', 'error',
         '-show_entries', 'format=duration',
         '-of', 'csv=p=0', mkv_path],
        capture_output=True, text=True)
    out = result.stdout.strip() if result.returncode == 0 else ''
    if out and out != 'N/A':
        return float(out)
    # Fallback: count packets (slow but handles truncated files)
    result = subprocess.run(
        ['ffprobe', '-v', 'error', '-count_packets',
         '-select_streams', 'v:0',
         '-show_entries', 'stream=nb_read_packets',
         '-of', 'csv=p=0', mkv_path],
        capture_output=True, text=True)
    out = result.stdout.strip() if result.returncode == 0 else ''
    if out and out != 'N/A':
        return int(out) / 30.0
    return 0.0


def count_mkv_frames_ffprobe(mkv_files, fps=30):
    """Count frames per MKV segment using duration * fps. Returns list."""
    from concurrent.futures import ThreadPoolExecutor
    counts = [0] * len(mkv_files)
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(_probe_duration, p): i
                   for i, p in enumerate(mkv_files)}
        for fut in futures:
            idx = futures[fut]
            counts[idx] = round(fut.result() * fps)
    return counts


def make_concat_file(mkv_files, concat_path):
    """Write ffmpeg concat demuxer file."""
    with open(concat_path, 'w') as f:
        for mkv_path in mkv_files:
            f.write(f"file '{mkv_path}'\n")


# ---------------------------------------------------------------------------
# CameraTimestamps
# ---------------------------------------------------------------------------

class CameraTimestamps:
    """Per-camera timestamp index for sync lookup via binary search."""

    def __init__(self, csv_path):
        entries = load_timestamps(csv_path)
        self.ros2_times = [e['ros2_time_ns'] for e in entries]
        self.count = len(self.ros2_times)
        self.start_ns = self.ros2_times[0] if self.count > 0 else 0
        self.end_ns = self.ros2_times[-1] if self.count > 0 else 0

    def find_closest(self, target_ns):
        """Return (csv_frame_idx, actual_ros2_time_ns) closest to target."""
        idx = bisect.bisect_left(self.ros2_times, target_ns)
        if idx == 0:
            return 0, self.ros2_times[0]
        if idx >= self.count:
            return self.count - 1, self.ros2_times[-1]
        if (target_ns - self.ros2_times[idx - 1]) <= (self.ros2_times[idx] - target_ns):
            return idx - 1, self.ros2_times[idx - 1]
        return idx, self.ros2_times[idx]


# ---------------------------------------------------------------------------
# SegmentMap
# ---------------------------------------------------------------------------

class SegmentMap:
    """Maps logical CSV frame index to MKV segment file and offset."""

    def __init__(self, mkv_files, csv_count):
        self.segment_counts = count_mkv_frames_ffprobe(mkv_files)
        self.mkv_files = mkv_files
        self.total_mkv = sum(self.segment_counts)
        self.pre_gate_skip = max(0, self.total_mkv - csv_count)

        self.cum_starts = []
        cum = 0
        for c in self.segment_counts:
            self.cum_starts.append(cum)
            cum += c

    def find_segment(self, csv_frame_idx):
        """Returns (segment_index, offset_within_segment)."""
        mkv_frame = csv_frame_idx + self.pre_gate_skip
        seg_idx = bisect.bisect_right(self.cum_starts, mkv_frame) - 1
        seg_idx = max(0, min(seg_idx, len(self.mkv_files) - 1))
        offset = mkv_frame - self.cum_starts[seg_idx]
        return seg_idx, offset


# ---------------------------------------------------------------------------
# CameraReader with background thread
# ---------------------------------------------------------------------------

class CameraReader:
    """Manages ffmpeg subprocess with a persistent background reader thread.

    The reader thread continuously reads frames into a ring buffer.
    The main loop picks the frame closest to a target csv_frame_idx,
    ensuring all cameras stay synchronized.
    """

    BUFFER_SIZE = 30  # ~1s at 30fps

    def __init__(self, cam_id, segment_map, thumb_w, thumb_h):
        self.cam_id = cam_id
        self.segment_map = segment_map
        self.thumb_w = thumb_w
        self.thumb_h = thumb_h
        self.frame_size = thumb_w * thumb_h * 3

        self._proc = None
        self._thread = None
        self._stop = threading.Event()
        self._gate = threading.Event()
        self._gate.set()

        self._lock = threading.Lock()
        self._buffer = collections.deque(maxlen=self.BUFFER_SIZE)
        self._last_served = (0, None)  # (csv_pos, frame) last returned
        self._csv_head = 0   # next csv_pos the reader will write
        self._eof = False

        self._concat_path = f'/tmp/play_cam{cam_id}_concat.txt'

    def start(self, from_csv_frame=0):
        """Start or restart ffmpeg from a given CSV frame index."""
        self._stop_internal()

        seg_idx, offset = self.segment_map.find_segment(from_csv_frame)

        remaining = self.segment_map.mkv_files[seg_idx:]
        make_concat_file(remaining, self._concat_path)

        seek_seconds = offset / 30.0

        vf = f'scale={self.thumb_w}:{self.thumb_h}'
        cmd = ['ffmpeg', '-v', 'error']
        if seek_seconds > 0.1:
            cmd += ['-ss', f'{seek_seconds:.3f}']
        cmd += ['-f', 'concat', '-safe', '0', '-i', self._concat_path,
                '-vf', vf,
                '-f', 'rawvideo', '-pix_fmt', 'bgr24', 'pipe:1']

        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=self.frame_size * 4)

        with self._lock:
            self._buffer.clear()
            self._csv_head = from_csv_frame
            self._last_served = (from_csv_frame, None)
            self._eof = False

        self._stop.clear()
        self._gate.set()
        self._thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._thread.start()

    def _reader_loop(self):
        """Background thread: read frames into ring buffer, throttled."""
        target_interval = 1.0 / 35.0  # slightly above 30fps max
        while not self._stop.is_set():
            self._gate.wait()
            if self._stop.is_set():
                break

            t0 = time.monotonic()

            data = self._proc.stdout.read(self.frame_size)
            if len(data) < self.frame_size:
                with self._lock:
                    self._eof = True
                break

            frame = np.frombuffer(data, dtype=np.uint8).reshape(
                self.thumb_h, self.thumb_w, 3).copy()

            with self._lock:
                csv_pos = self._csv_head
                self._buffer.append((csv_pos, frame))
                self._csv_head = csv_pos + 1

            # Throttle: don't read faster than needed
            elapsed = time.monotonic() - t0
            sleep = target_interval - elapsed
            if sleep > 0.001:
                time.sleep(sleep)

    def get_frame_at(self, target_csv_idx):
        """Get frame closest to target_csv_idx from the buffer.

        Consumes (removes) frames older than the selected one.
        Returns (csv_pos, frame).  If buffer is empty, returns last served.
        """
        with self._lock:
            if not self._buffer:
                return self._last_served

            # Find the entry closest to target
            best_i = 0
            best_dist = abs(self._buffer[0][0] - target_csv_idx)
            for i in range(1, len(self._buffer)):
                d = abs(self._buffer[i][0] - target_csv_idx)
                if d <= best_dist:
                    best_dist = d
                    best_i = i
                else:
                    break  # buffer is sorted, distance increasing = past best

            best_pos, best_frame = self._buffer[best_i]

            # Remove frames up to and including the selected one
            for _ in range(best_i + 1):
                self._buffer.popleft()

            self._last_served = (best_pos, best_frame)
            return best_pos, best_frame

    def get_latest(self):
        """Non-blocking: return latest frame regardless of target."""
        with self._lock:
            if self._buffer:
                pos, frame = self._buffer[-1]
                self._last_served = (pos, frame)
                return pos, frame
            return self._last_served

    @property
    def eof(self):
        with self._lock:
            return self._eof

    def pause(self):
        self._gate.clear()

    def resume(self):
        self._gate.set()

    def seek_to(self, csv_frame_idx):
        self.start(from_csv_frame=csv_frame_idx)

    def _stop_internal(self):
        self._stop.set()
        self._gate.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None
        if self._proc is not None:
            try:
                self._proc.stdout.close()
                self._proc.kill()
                self._proc.wait()
            except Exception:
                pass
            self._proc = None

    def cleanup(self):
        self._stop_internal()
        try:
            os.unlink(self._concat_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# SyncController
# ---------------------------------------------------------------------------

class SyncController:
    """Master timeline and playback state."""

    PLAYING = 'playing'
    PAUSED = 'paused'
    SPEEDS = [0.25, 0.5, 1.0, 2.0, 4.0]

    def __init__(self, cam_timestamps):
        self.timestamps = cam_timestamps
        self.state = self.PAUSED
        self.speed = 1.0

        self.common_start = max(ts.start_ns for ts in cam_timestamps.values())
        self.common_end = min(ts.end_ns for ts in cam_timestamps.values())
        self.duration_ns = self.common_end - self.common_start
        self.current_ns = self.common_start

        self._wall_ref = 0.0
        self._time_ref = 0

    def play(self):
        self._wall_ref = time.monotonic()
        self._time_ref = self.current_ns
        self.state = self.PLAYING

    def pause(self):
        self.update()
        self.state = self.PAUSED

    def toggle(self):
        if self.state == self.PLAYING:
            self.pause()
        else:
            self.play()

    def update(self):
        if self.state != self.PLAYING:
            return False
        elapsed = time.monotonic() - self._wall_ref
        self.current_ns = self._time_ref + int(elapsed * self.speed * 1e9)
        self.current_ns = max(self.common_start,
                              min(self.current_ns, self.common_end))
        if self.current_ns >= self.common_end:
            self.current_ns = self.common_end
            self.state = self.PAUSED
        return True

    def seek(self, target_ns):
        self.current_ns = max(self.common_start,
                              min(target_ns, self.common_end))
        if self.state == self.PLAYING:
            self._wall_ref = time.monotonic()
            self._time_ref = self.current_ns

    def seek_relative(self, delta_s):
        self.seek(self.current_ns + int(delta_s * 1e9))

    def speed_up(self):
        idx = self.SPEEDS.index(self.speed) if self.speed in self.SPEEDS else 2
        if idx < len(self.SPEEDS) - 1:
            self.speed = self.SPEEDS[idx + 1]
            if self.state == self.PLAYING:
                self._wall_ref = time.monotonic()
                self._time_ref = self.current_ns

    def speed_down(self):
        idx = self.SPEEDS.index(self.speed) if self.speed in self.SPEEDS else 2
        if idx > 0:
            self.speed = self.SPEEDS[idx - 1]
            if self.state == self.PLAYING:
                self._wall_ref = time.monotonic()
                self._time_ref = self.current_ns

    def get_targets(self):
        targets = {}
        for cam_id, ts in self.timestamps.items():
            targets[cam_id] = ts.find_closest(self.current_ns)
        return targets

    def progress(self):
        if self.duration_ns <= 0:
            return 0.0
        return (self.current_ns - self.common_start) / self.duration_ns

    def elapsed_str(self):
        elapsed_s = (self.current_ns - self.common_start) / 1e9
        total_s = self.duration_ns / 1e9
        return f'{_fmt_time(elapsed_s)} / {_fmt_time(total_s)}'


def _fmt_time(seconds):
    m, s = divmod(int(seconds), 60)
    return f'{m:02d}:{s:02d}'


# ---------------------------------------------------------------------------
# GridDisplay
# ---------------------------------------------------------------------------

class GridDisplay:
    """Composites camera thumbnails into a grid with OSD overlays."""

    COLORS = [
        (0, 255, 0),    # green
        (255, 200, 0),  # cyan
        (0, 200, 255),  # orange
        (255, 0, 255),  # magenta
        (0, 255, 255),  # yellow
        (255, 128, 0),  # blue
    ]

    def __init__(self, cam_ids, display_w, display_h, cols, rows,
                 src_w, src_h):
        self.cam_ids = sorted(cam_ids)
        self.cols = cols
        self.rows = rows
        self.display_w = display_w
        self.display_h = display_h

        self.cell_w = display_w // cols
        self.cell_h = display_h // rows

        aspect = src_w / src_h
        self.thumb_w = self.cell_w
        self.thumb_h = int(self.cell_w / aspect)
        if self.thumb_h > self.cell_h:
            self.thumb_h = self.cell_h
            self.thumb_w = int(self.cell_h * aspect)

        self.thumb_w = self.thumb_w // 2 * 2
        self.thumb_h = self.thumb_h // 2 * 2

        self.pad_top = (self.cell_h - self.thumb_h) // 2
        self.pad_left = (self.cell_w - self.thumb_w) // 2

        self.canvas = np.zeros((display_h, display_w, 3), dtype=np.uint8)

    def compose(self, thumbnails, targets, sync_info, playback_info):
        self.canvas[:] = 0

        for i, cam_id in enumerate(self.cam_ids):
            row = i // self.cols
            col = i % self.cols
            y = row * self.cell_h + self.pad_top
            x = col * self.cell_w + self.pad_left

            thumb = thumbnails.get(cam_id)
            if thumb is not None:
                h, w = thumb.shape[:2]
                self.canvas[y:y + h, x:x + w] = thumb

            color = self.COLORS[i % len(self.COLORS)]
            frame_idx = targets[cam_id][0] if cam_id in targets else 0
            cv2.putText(self.canvas, f'cam{cam_id} F:{frame_idx}',
                        (x + 5, y + 20),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.55, color, 1,
                        cv2.LINE_AA)

            delta_ms = sync_info['deltas_ms'].get(cam_id, 0.0)
            sign = '+' if delta_ms >= 0 else ''
            cv2.putText(self.canvas, f'{sign}{delta_ms:.1f}ms',
                        (x + 5, y + 42),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.45, color, 1,
                        cv2.LINE_AA)

        bar_y = self.display_h - 40
        state_str = 'PLAYING' if playback_info['state'] == 'playing' else 'PAUSED'
        speed_str = f'{playback_info["speed"]:.2g}x'
        fps_str = f'{playback_info["fps"]:.0f}fps'
        spread_str = f'Spread:{sync_info["spread_ms"]:.1f}ms'
        elapsed_str = playback_info['elapsed_str']

        osd = f'[{state_str}] {speed_str}  {elapsed_str}  {spread_str}  {fps_str}'
        cv2.putText(self.canvas, osd,
                    (10, bar_y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (220, 220, 220), 1,
                    cv2.LINE_AA)

        prog_y = self.display_h - 10
        prog_w = self.display_w - 20
        cv2.rectangle(self.canvas, (10, prog_y - 4),
                      (10 + prog_w, prog_y + 4), (60, 60, 60), -1)
        filled = int(prog_w * playback_info['progress'])
        if filled > 0:
            spread = sync_info['spread_ms']
            bar_color = ((0, 200, 0) if spread < 50
                         else (0, 200, 200) if spread < 100
                         else (0, 0, 200))
            cv2.rectangle(self.canvas, (10, prog_y - 4),
                          (10 + filled, prog_y + 4), bar_color, -1)

        return self.canvas


# ---------------------------------------------------------------------------
# FPS counter
# ---------------------------------------------------------------------------

class FPSCounter:
    def __init__(self, window=30):
        self.window = window
        self.times = []
        self.fps = 0.0

    def tick(self):
        now = time.monotonic()
        self.times.append(now)
        if len(self.times) > self.window:
            self.times.pop(0)
        if len(self.times) >= 2:
            self.fps = (len(self.times) - 1) / (self.times[-1] - self.times[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Synchronized 6-camera playback tool')
    parser.add_argument('input_dir',
                        help='Directory containing MKV and CSV files')
    parser.add_argument('--speed', type=float, default=1.0,
                        help='Initial playback speed (default: 1.0)')
    parser.add_argument('--layout', default='3x2', choices=['3x2', '2x3'],
                        help='Grid layout: 3x2 or 2x3 (default: 3x2)')
    parser.add_argument('--session', default=None,
                        help='Recording session timestamp (default: latest)')
    parser.add_argument('--cameras', default=None,
                        help='Comma-separated camera IDs (default: all)')
    parser.add_argument('--width', type=int, default=1920,
                        help='Source frame width (default: 1920)')
    parser.add_argument('--height', type=int, default=1280,
                        help='Source frame height (default: 1280)')
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f'ERROR: {args.input_dir} is not a directory')
        sys.exit(1)

    cameras = discover_cameras(args.input_dir, session=args.session)
    if not cameras:
        print('ERROR: No camera data found')
        sys.exit(1)

    if args.cameras:
        selected = set(int(x) for x in args.cameras.split(','))
        cameras = {k: v for k, v in cameras.items() if k in selected}

    valid = {k: v for k, v in cameras.items() if v.get('csv_file')}
    if not valid:
        print('ERROR: No cameras with valid CSV files')
        sys.exit(1)

    cam_ids = sorted(valid.keys())
    print(f'Found {len(cam_ids)} cameras: {cam_ids}')

    if args.layout == '3x2':
        cols, rows = 3, 2
    else:
        cols, rows = 2, 3

    # Load timestamps
    print('Loading timestamps...')
    cam_ts = {}
    for cam_id in cam_ids:
        cam_ts[cam_id] = CameraTimestamps(valid[cam_id]['csv_file'])
        print(f'  cam{cam_id}: {cam_ts[cam_id].count} frames, '
              f'{(cam_ts[cam_id].end_ns - cam_ts[cam_id].start_ns) / 1e9:.1f}s')

    # Build segment maps
    print('Building segment maps (ffprobe)...')
    seg_maps = {}
    for cam_id in cam_ids:
        seg_maps[cam_id] = SegmentMap(
            valid[cam_id]['mkv_files'], cam_ts[cam_id].count)
        print(f'  cam{cam_id}: {seg_maps[cam_id].total_mkv} MKV frames, '
              f'skip={seg_maps[cam_id].pre_gate_skip}')

    # Create display
    display = GridDisplay(cam_ids, 1920, 1080, cols, rows,
                          args.width, args.height)
    print(f'Display: {display.display_w}x{display.display_h}, '
          f'thumb={display.thumb_w}x{display.thumb_h}, '
          f'grid={cols}x{rows}')

    # Create sync controller
    sync = SyncController(cam_ts)
    sync.speed = args.speed
    print(f'Common range: {sync.duration_ns / 1e9:.1f}s')

    # Create and start camera readers
    readers = {}
    for cam_id in cam_ids:
        readers[cam_id] = CameraReader(
            cam_id, seg_maps[cam_id],
            display.thumb_w, display.thumb_h)

    print('Starting ffmpeg readers...')
    start_targets = sync.get_targets()
    for cam_id, (frame_idx, _) in start_targets.items():
        readers[cam_id].start(from_csv_frame=frame_idx)

    # Wait for initial frames from all readers
    print('Waiting for first frames...')
    deadline = time.monotonic() + 5.0
    current_frames = {}
    while time.monotonic() < deadline and len(current_frames) < len(cam_ids):
        for cam_id in cam_ids:
            if cam_id not in current_frames:
                csv_pos, frame = readers[cam_id].get_latest()
                if frame is not None:
                    current_frames[cam_id] = frame
        time.sleep(0.05)
    print(f'Got initial frames from {len(current_frames)}/{len(cam_ids)} cameras')

    # Pause all readers (wait for Space to start)
    for r in readers.values():
        r.pause()

    # Display window
    cv2.namedWindow('6-Camera Sync Playback', cv2.WINDOW_NORMAL)
    cv2.resizeWindow('6-Camera Sync Playback', 1920, 1080)

    fps_counter = FPSCounter()
    print('Ready. Press Space to play, q to quit.')

    try:
        while True:
            sync.update()
            targets = sync.get_targets()

            # Pick frames matching sync target from each camera's buffer
            served_positions = {}
            for cam_id in cam_ids:
                target_idx = targets[cam_id][0]
                csv_pos, frame = readers[cam_id].get_frame_at(target_idx)
                served_positions[cam_id] = csv_pos
                if frame is not None:
                    current_frames[cam_id] = frame

            # Compute sync quality using actual ROS2 timestamps
            # delta = displayed frame's ros2_time - master time
            master_ns = sync.current_ns
            deltas = {}
            for cam_id in cam_ids:
                pos = served_positions[cam_id]
                pos = max(0, min(pos, cam_ts[cam_id].count - 1))
                frame_ns = cam_ts[cam_id].ros2_times[pos]
                deltas[cam_id] = (frame_ns - master_ns) / 1e6  # ms
            delta_vals = list(deltas.values())
            spread = max(delta_vals) - min(delta_vals) if delta_vals else 0.0
            sync_info = {'deltas_ms': deltas, 'spread_ms': spread}

            # Compose and display
            fps_counter.tick()
            playback_info = {
                'speed': sync.speed,
                'state': sync.state,
                'fps': fps_counter.fps,
                'progress': sync.progress(),
                'elapsed_str': sync.elapsed_str(),
            }
            canvas = display.compose(current_frames, targets,
                                     sync_info, playback_info)
            cv2.imshow('6-Camera Sync Playback', canvas)

            # Keyboard handling
            wait_ms = 1 if sync.state == SyncController.PLAYING else 30
            key = cv2.waitKey(wait_ms) & 0xFF

            if key == ord('q') or key == 27:
                break
            elif key == ord(' '):
                if sync.state == SyncController.PLAYING:
                    sync.pause()
                    for r in readers.values():
                        r.pause()
                else:
                    for r in readers.values():
                        r.resume()
                    sync.play()
            elif key == 83 or key == ord('d'):  # Right arrow
                sync.seek_relative(5.0)
                targets = sync.get_targets()
                for cam_id, (fi, _) in targets.items():
                    readers[cam_id].seek_to(fi)
                if sync.state == SyncController.PAUSED:
                    # Read one frame then pause
                    time.sleep(0.2)
                    for cam_id in cam_ids:
                        csv_pos, frame = readers[cam_id].get_latest()
                        if frame is not None:
                            current_frames[cam_id] = frame
                    for r in readers.values():
                        r.pause()
            elif key == 81 or key == ord('a'):  # Left arrow
                sync.seek_relative(-5.0)
                targets = sync.get_targets()
                for cam_id, (fi, _) in targets.items():
                    readers[cam_id].seek_to(fi)
                if sync.state == SyncController.PAUSED:
                    time.sleep(0.2)
                    for cam_id in cam_ids:
                        csv_pos, frame = readers[cam_id].get_latest()
                        if frame is not None:
                            current_frames[cam_id] = frame
                    for r in readers.values():
                        r.pause()
            elif key == ord('+') or key == ord('='):
                sync.speed_up()
            elif key == ord('-'):
                sync.speed_down()
            elif key == ord('.') and sync.state == SyncController.PAUSED:
                # Step forward 1 frame
                ref_cam = cam_ids[0]
                csv_pos, _ = readers[ref_cam].get_latest()
                if csv_pos + 1 < cam_ts[ref_cam].count:
                    next_ns = cam_ts[ref_cam].ros2_times[csv_pos + 1]
                    sync.seek(next_ns)
                    targets = sync.get_targets()
                    for cam_id, (fi, _) in targets.items():
                        readers[cam_id].seek_to(fi)
                    time.sleep(0.2)
                    for cam_id in cam_ids:
                        p, f = readers[cam_id].get_latest()
                        if f is not None:
                            current_frames[cam_id] = f
                    for r in readers.values():
                        r.pause()
            elif key == ord(',') and sync.state == SyncController.PAUSED:
                # Step backward 1 frame
                ref_cam = cam_ids[0]
                csv_pos, _ = readers[ref_cam].get_latest()
                if csv_pos > 0:
                    prev_ns = cam_ts[ref_cam].ros2_times[csv_pos - 1]
                    sync.seek(prev_ns)
                    targets = sync.get_targets()
                    for cam_id, (fi, _) in targets.items():
                        readers[cam_id].seek_to(fi)
                    time.sleep(0.2)
                    for cam_id in cam_ids:
                        p, f = readers[cam_id].get_latest()
                        if f is not None:
                            current_frames[cam_id] = f
                    for r in readers.values():
                        r.pause()

    except KeyboardInterrupt:
        pass
    finally:
        print('\nStopping...')
        for r in readers.values():
            r.cleanup()
        cv2.destroyAllWindows()
        print('Done.')


if __name__ == '__main__':
    main()
