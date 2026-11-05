<img src="logo.png" alt="" width="96">

# VOD Failover

A plugin for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).

When a VOD stream fails to start on one provider account (for example HTTP 458, 404 or a timeout), this retries the same title on the next account that carries it. It remembers the failure, so later requests go straight to a working account.

Tested on Dispatcharr 0.33.0.

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

## Changelog

### 1.4.0
- Added an optional Streamcache setting: when every account is busy, it can free a slot by cancelling downloads nobody is watching.
- Later, without a new version number: accounts are now told apart by host and port, so several accounts behind one local proxy work.

### 1.3.0
- Fixed retries joining a failed session that Dispatcharr was keeping open for a moment.
- If a provider rejects a different URL than expected, the failure is now matched by host and stream ID.

### 1.2.0
- For episodes, the plugin now loads the series' episode list from other accounts, because Dispatcharr may not know they carry that episode yet. Each provider is asked at most once per 30 minutes for the same series.

### 1.1.0
- Connection-limit errors are now remembered separately, for a number of seconds you can set.

### 1.0.0
- First version: retries a failed VOD stream on the next account that carries the title, and remembers the failure.
- Added **Check Hook Status** and **Clear Remembered Failures**.

## Licence

MIT
