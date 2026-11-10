<img src="logo.png" alt="" width="96">

# Failover Manager

A plugin for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr). Formerly called VOD Failover.

When a VOD stream fails to start on one provider account (for example HTTP 458, 404 or a timeout), this retries the same title on the next account that carries it. It remembers the failure, so later requests go straight to a working account.

Tested on Dispatcharr 0.33.0.

Works with [M3U Failover](https://github.com/Brxdy22/Dispatcharr-M3U-Failover) and [Gluetun AutoSwitch](https://github.com/Brxdy22/Dispatcharr-Gluetun-AutoSwitch-Plugin). All three together cover provider URL failover, account failover for VOD, and VPN exit switching.

See [Dispatcharr Failover Stack](https://github.com/Brxdy22/Dispatcharr-Failover-Stack) to install all three side by side.

## Install

Copy this repository's files into `/data/plugins/failover_manager` in your Dispatcharr data folder, then enable **Failover Manager** on the Plugins page.

Or download `failover_manager-1.6.zip` from Releases and import it on the Plugins page. Use that zip rather than GitHub's source zip, because Dispatcharr names the plugin after the zip's top folder.

## Upgrading from VOD Failover

1.6 renamed the plugin, so Dispatcharr sees it as a new plugin with its own settings.

1. Note your settings in **VOD Failover**, then remove it on the Plugins page (or delete `/data/plugins/vod_failover`).
2. Install **Failover Manager** as above, enable it and enter your settings again. Use **Load Mirror List** to fill the Mirror list box again.
3. Gluetun AutoSwitch 1.8.4.2 or later reads the new log lines. Older versions only read the old ones, so update it too.

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

## More by Brxdy22

- [M3U Failover](https://github.com/Brxdy22/Dispatcharr-M3U-Failover): Proxy that gives an M3U account several provider URLs and fails over between them. Docker image: [brxdy22/m3u-failover](https://hub.docker.com/r/brxdy22/m3u-failover).
- [Gluetun AutoSwitch](https://github.com/Brxdy22/Dispatcharr-Gluetun-AutoSwitch-Plugin): Moves Gluetun to another VPN exit when streams slow down, VOD is refused or speed tests fail.
- [Clean Titles](https://github.com/Brxdy22/Dispatcharr-Clean-Titles-Plugin): Renames VOD movies and series to their official TMDB titles and tidies provider categories.
- [Teamarr Assistant](https://github.com/Brxdy22/Dispatcharr-Teamarr-Assistant-Plugin): Tags regional sports networks with their teams and starts Teamarr generations when new streams appear.

## Release notes

See [Releases](https://github.com/Brxdy22/Dispatcharr-Failover-Manager-Plugin/releases/latest).

## Licence

MIT
