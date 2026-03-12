# PTS Compress — HLS Video Compression Worker

GitHub Actions worker that converts videos to HLS (H.264 540p) format.

Used by [PTS Library](https://github.com/ptststemp-rgb/mydrive) for video streaming.

## What it does

1. Downloads video from SpaceByte
2. Encodes to H.264 540p HLS (fmp4 segments, 4s each, CRF 26, AAC 128k)
3. Parallel-uploads all HLS segments to SpaceByte folder
4. Deletes original source file
5. Callbacks Oracle VPS to update database

## Trigger

Triggered via `workflow_dispatch` from the GCP compression dashboard or VPS.
