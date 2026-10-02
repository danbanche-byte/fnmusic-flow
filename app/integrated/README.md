# 内置平台服务

`pockettune-server` 是本项目内置的 PocketTune 服务端能力，作为同一个 Docker 容器中的内部进程运行。用户不需要单独安装 PocketTune。

它提供网易云增强 API、网易云登录/歌单接口，以及 PocketTune 原有的 QQ 音乐歌词/搜索模块。飞牛音乐仍通过开放 API 作为外部目标系统连接。

该目录保留上游许可证和版权信息；构建镜像时安装其 `server/package.json` 中的运行依赖。
