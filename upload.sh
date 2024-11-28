rclone sync --exclude=".*" --progress --s3-no-check-bucket --s3-chunk-size=256M outputs/data r2://fieldmaps-data-cod/data
rclone sync --exclude=".*" --progress --s3-no-check-bucket --s3-chunk-size=256M outputs/stac r2://fieldmaps-data-cod/stac
