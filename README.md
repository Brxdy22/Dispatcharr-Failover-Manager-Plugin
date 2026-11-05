# VOD Failover

A plugin for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).

When a VOD stream fails to start on one provider account (for example HTTP 458, 404 or a timeout), this retries the same title on the next account that carries it. It remembers the failure, so later requests go straight to a working account.

## Install

Copy this repository's files into `/data/plugins/vod_failover` in your Dispatcharr data folder, then enable **VOD Failover** on the Plugins page.

Or download `vod_failover-1.4.0.zip` from Releases and import it on the Plugins page. Use that zip rather than GitHub's source zip, because Dispatcharr names the plugin after the zip's top folder.

## Use

1. Enable it in the plugin settings. It needs at least two accounts carrying the same titles.
2. **Check Hook Status** confirms it is active.
3. **Clear Remembered Failures** resets what it has learned.

## Notes

- It patches Dispatcharr's internal VOD functions, so a Dispatcharr update can break it. Check the hook status after updating.
- The Streamcache settings are optional and only matter if you run a Streamcache service.

## Licence

MIT
