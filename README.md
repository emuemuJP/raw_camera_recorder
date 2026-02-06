# raw_camera_recorder

GStreamer で RAW 映像 (UYVY) を MKV 保存しつつ、各フレームの ROS2 タイムスタンプを CSV に記録する ROS2 Foxy ノード。
Jetson AGX Orin + USB カメラ 6 台での同時録画を想定。

## アーキテクチャ

```
v4l2src (UYVY 30fps) --> queue --> [pad probe] --> splitmuxsink --> MKV ファイル
                                       |
                                 CSV (20fps ROS2 timestamps)
```

### 設計上の制約と対策

| 問題 | 原因 | 対策 |
|------|------|------|
| ランダムに1台のカメラが脱落 | `videorate` 要素の内部バッファリングとCPU負荷 | Bresenham式フレームドロップで代替 (pad probe内) |
| 同上 | `tee + appsink` によるバッファコピー2倍化 | pad probe でタイムスタンプ取得 (tee/appsink不要) |
| PLAYING遷移の長時間化 | pre-gate フレームを `GST_PAD_PROBE_DROP` するとsplitmuxsinkがバッファを受け取れず遷移がブロック | pre-gate フレームは MKV に通す (CSV には書かない) |
| CSV フレーム欠落 (MKV > CSV) | `cam.stop` フラグによるシャットダウンがEOS前にframe_loopを終了 | EOS ベースのシャットダウンに変更 |
| カメラ開始時刻のバラつき | 順次PLAYING遷移による録画開始時刻の差 | ソフトウェアゲート (`atomic<bool> recording`) で一斉開始 |

### pad probe の3機能

1. **ソフトウェアゲート**: `recording == false` の間は CSV に書かず MKV のみ記録
2. **FPS 削減**: Bresenham アキュムレータで 30fps -> 20fps (1/3 フレームを `GST_PAD_PROBE_DROP`)
3. **CSV 書き込み**: 保持フレームの `frame_index`, `gst_pts_ns`, `ros2_time_ns` を記録

### 起動シーケンス

```
Phase 1: パイプライン作成 + pad probe 接続 + PAUSED (500ms間隔)
Phase 2: 順次 PLAYING 遷移 (500ms間隔)
Phase 3: 3秒待機後、全カメラの recording=true を一斉セット
Duration Timer: EOS 送信 -> 3秒待機 -> rclcpp::shutdown()
```

## ファイル構成

```
src/raw_camera_recorder/
  CMakeLists.txt
  package.xml
  README.md                            # 本ファイル
  include/raw_camera_recorder/
    raw_camera_recorder.hpp            # ノードクラス宣言
  src/
    raw_camera_recorder.cpp            # メインロジック
    raw_camera_recorder_main.cpp       # エントリポイント
  cfg/
    params.yaml                        # デフォルトパラメータ
  launch/
    raw_6cameras.launch.py             # 6カメラ用launchファイル
  scripts/
    mkv_to_rosbag.py                   # MKV+CSV -> MCAP rosbag 変換
```

## ROS2 パラメータ

| パラメータ | デフォルト | 説明 |
|---|---|---|
| `camera_count` | 6 | カメラ台数 |
| `device_base` | `/dev/video` | デバイスパスのベース |
| `device_index` | 0 | 開始デバイスインデックス |
| `width` | 1920 | 幅 (px) |
| `height` | 1280 | 高さ (px) |
| `capture_fps` | 30 | v4l2src キャプチャ FPS |
| `record_fps` | 20 | 保存 FPS (< capture_fps なら pad probe でドロップ) |
| `pixel_format` | `UYVY` | `UYVY` or `I420` |
| `output_dir` | `/mnt/ssd/raw_record` | 出力ディレクトリ |
| `segment_duration_ns` | 10000000000 | MKV セグメント分割間隔 (10秒) |
| `duration_sec` | 0 | 録画秒数 (0 = 無制限, Ctrl+C で停止) |
| `io_mode` | 2 | v4l2 io-mode (2=mmap) |

## 出力ファイル

### MKV (RAW映像)

```
cam0_raw_202602041957_00000.mkv   # セグメント0 (最大10秒分)
cam0_raw_202602041957_00001.mkv   # セグメント1
...
```

- コーデック: rawvideo (UYVY)
- pre-gate フレーム (30fps) + 録画フレーム (20fps) を含む
- `ffprobe` で確認可能

### CSV (タイムスタンプ)

```
cam0_timestamps_202602041957.csv
```

フォーマット:
```csv
frame_index,gst_pts_ns,ros2_time_ns
0,5100000000,1770202671620003286
1,5200000000,1770202671720003286
...
```

- `frame_index`: 0始まりの連番 (録画開始からのフレーム番号)
- `gst_pts_ns`: GStreamer PTS (ナノ秒) — MKV フレームとの照合キー
- `ros2_time_ns`: ROS2 タイムスタンプ (ナノ秒) — rosbag 変換時に使用

### MKV と CSV の関係

MKV には pre-gate フレームが含まれるため、MKV フレーム数 > CSV フレーム数 となる。
rosbag 変換時は CSV の `gst_pts_ns` を使って MKV 内の該当フレームを特定する。

```
MKV:  [pre-gate 30fps ...] [recording 20fps .................]
CSV:                        [recording 20fps .................]
```

## ビルドと実行

```bash
# ビルド
cd /home/agv/ros2_foxy_ws
colcon build --packages-select raw_camera_recorder

# 10秒録画 (20fps)
source install/setup.bash
ros2 run raw_camera_recorder raw_camera_recorder_main \
  --ros-args -p output_dir:=/mnt/ssd/raw_record -p duration_sec:=10 -p record_fps:=20

# launch ファイルで実行
ros2 launch raw_camera_recorder raw_6cameras.launch.py

# 後処理 (MKV+CSV -> MCAP rosbag, JPEG圧縮, 6カメラ並列)
python3 src/raw_camera_recorder/scripts/mkv_to_rosbag.py \
  /mnt/ssd/raw_record -o /mnt/ssd/raw_record/bag

# RAW BGR モード (非圧縮, 低速)
python3 src/raw_camera_recorder/scripts/mkv_to_rosbag.py \
  /mnt/ssd/raw_record -o /mnt/ssd/raw_record/bag --raw
```

## テスト結果

### 条件

- Jetson AGX Orin, Ubuntu 20.04, ROS2 Foxy, GStreamer 1.16.3
- USB カメラ 6 台 (UYVY 1920x1280 @ 30fps)
- 出力先: /mnt/ssd/raw_record (USB SSD)
- 録画時間: 10 秒, record_fps: 20

### 結果 (2回連続成功)

| カメラ | CSV frames | 実効 FPS | 開始時刻offset | pre-gate | fps-dropped |
|--------|-----------|---------|---------------|----------|-------------|
| cam0 | 194 | 19.4 | 0ms | 290 | 97 |
| cam1 | 193 | 19.3 | 0ms | 195 | 97 |
| cam2 | 194 | 19.4 | 0ms | 150 | 97 |
| cam3 | 190 | 19.0 | -5ms | 137 | 96 |
| cam4 | 189 | 18.9 | -100ms | 101 | 95 |
| cam5 | 188 | 18.8 | +3ms | 93 | 94 |

- 6 台全カメラ動作: 2 回連続成功
- MKV/CSV フレーム整合性: MKV = pre-gate + CSV (完全一致)
- CSV 開始時刻同期: 5 カメラが 5ms 以内, cam4 のみ 100ms (Bresenham パターンの位相差)
- FPS ドロップ比: CSV / (CSV + fps-dropped) = 2/3 (30->20fps として正確)

## MCAP 変換テスト結果 (2分録画)

### 条件

- 入力: 6カメラ × 2分録画 (MKV 78ファイル, 69GB)
- 変換モード: JPEG (q=2, 最高品質)
- 並列ワーカー: 6 (カメラ数と同じ)

### 結果

| カメラ | CSV frames | MCAP サイズ |
|--------|-----------|------------|
| cam0 | 2288 | 61 MB |
| cam1 | 2321 | 91 MB |
| cam2 | 2300 | 91 MB |
| cam3 | 2282 | 81 MB |
| cam4 | 2296 | 86 MB |
| cam5 | 2292 | 165 MB |

- 合計: 13,779 フレーム, 575 MB, **205秒**
- 出力: `bag/` ディレクトリ (6 MCAP + metadata.yaml)
- 参考: RAW BGR モードは cam0 のみで 3,159秒 (JPEG の 42倍遅い)

## 未検証事項

- `ros2 bag play` + `rqt_image_view` での再生確認 (Foxy は MCAP 非対応, Foxglove Studio で確認可能)
- 長時間録画 (10 分以上) での安定性

## 帯域幅の参考値

```
1カメラ UYVY 1920x1280:
  30fps (v4l2src): 1920 * 1280 * 2 * 30 = ~147 MB/s
  20fps (MKV保存): 1920 * 1280 * 2 * 20 = ~98 MB/s

6カメラ合計:
  USB入力 (30fps): ~884 MB/s
  SSD書き込み (20fps): ~590 MB/s
```
