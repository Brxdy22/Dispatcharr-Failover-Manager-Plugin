<img src="logo.png" alt="" width="96">

# VOD Failover

A plugin for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).

When a VOD stream fails to start on one provider account (for example HTTP 458, 404 or a timeout), this retries the same title on the next account that carries it. It remembers the failure, so later requests go straight to a working account.

Tested on Dispatcharr 0.33.0.

## Install

Copy this repository's files into `/data/plugins/vod_failover` in your Dispatcharr data folder, then enable **VOD Failover** on the Plugins page.

Or download `vod_failover-1.5.zip` from Releases and import it on the Plugins page. Use that zip rather than GitHub's source zip, because Dispatcharr names the plugin after the zip's top folder.

## Use

1. Enable it in the plugin settings. It needs at least two accounts carrying the same titles.
2. **Check Hook Status** confirms it is active.
3. **Clear Remembered Failures** resets what it has learned.

## Mirror URLs

If you run [M3U Failover](https://github.com/Brxdy22/Dispatcharr-M3U-Failover) 1.3 or later, the **Mirror URLs** section manages its URL lists from this page. Every account whose Server URL points at the **M3U Failover address** is listed.

- **Mirror Status** shows the URL each account uses, pauses, resting URLs, streams playing and the last sort.
- **Load Mirror List** copies the proxy's lists into the **Mirror list** box. Reopen the page to see them.
- **Apply Mirror List** sends the saved box to the proxy, which updates `upstreams.conf` and uses the new order at once. Edit, press **Save**, then **Apply**. Accounts left out of the box are not changed.
- **Sort Mirrors Now** runs the proxy's nightly sort straight away.

## Notes

- It patches Dispatcharr's internal VOD functions, so a Dispatcharr update can break it. Check the hook status after updating.
- The Streamcache settings are optional and only matter if you run a Streamcache service.

## Release notes

See [Releases](https://github.com/Brxdy22/Dispatcharr-VOD-Failover-Plugin/releases/latest).

## Licence

MIT
