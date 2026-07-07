# Orchard

> [!IMPORTANT]
>
> **macOS 15 (Sequoia) or later**
>
> The  [newly introduced "Local Network" permission](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy) in macOS Sequoia requires accepting a GUI pop-up on each host machine that runs the Orchard Worker.
>
> To work around this, there are two options. The first one is to invoke the `orchard worker run` as `root` with an additional `--user` command-line argument, which takes a name of your regular, non-privileged user on the host machine.
>
> This will cause the Orchard Worker to start a small `orchard localnetworkhelper` process in the background and then drop the privileges to the specified user.
>
>The helper process is privileged and needed to establish network connections on behalf of the Orchard Worker without triggering a GUI pop-up.
>
>This approach is more secure than simply running `orchard worker run` as `root`, because only a small part of Orchard Worker runs privileged and the only functionality that this part has is establishing new connections.
>
> The second workaround is to [set local the network privacy preferences](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy#macOS-considerations) so that all [IPv4 private address space](https://datatracker.ietf.org/doc/html/rfc1918#section-3) that could potentially be used for VMs is excluded:
>
> ```shell
> sudo defaults write com.apple.network.local-network AllowedEthernetLocalNetworkAddresses -array "10.0.0.0/8" "172.16.0.0/12" "192.168.0.0/16"
> sudo defaults write com.apple.network.local-network AllowedWiFiLocalNetworkAddresses -array "10.0.0.0/8" "172.16.0.0/12" "192.168.0.0/16"
> ```
>
> ...and then reboot.

<img src="https://github.com/cirruslabs/orchard/raw/main/docs/OrchardSocial.png"/>

Orchard is an orchestration system for [Tart](https://github.com/cirruslabs/tart). Create a cluster of bare-metal Apple Silicon machines and manage dozens of VMs with ease!

## Usage

The fastest way to get started with Orchard is to use a local development mode:

```shell
brew install openai/tools/orchard
orchard dev
```

This will start Orchard Controller and a single Orchard Worker on your local machine.

You can interact with the newly created cluster using the `orchard` CLI or programmatically, through the built-in REST API server.

Please check out the [official documentation](https://tart.run/orchard/quick-start/) for more information and/or feel free to use [issues](https://github.com/cirruslabs/orchard/issues) for the remaining questions.
