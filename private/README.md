# Box Mount binary (you provide it)

Box Mount is in preview and isn't included in this repository. [Request access to the Box Mount
preview](https://bit.ly/box-mount-preview) (participants get the binary and setup guidance), then
put the **Linux arm64** build here:

```
private/box-mount-0.8.0/box-mount
```

The sandbox image copies it from that path (`docker/Dockerfile`), and `just up` stops with a
message if it's missing. Everything in `private/` except this file is git-ignored, so the binary
and any release archive you unpack here stay out of git.

The demo was built and tested with Box Mount 0.8.0. To use another version, put it at the same
path, or change the `COPY` line in `docker/Dockerfile`.
