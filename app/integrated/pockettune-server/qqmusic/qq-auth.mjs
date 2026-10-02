import { randomUUID } from "node:crypto";
import { cookieObjectToHeader, getQQAccountId } from "./runtime-store.mjs";

const QM_API_URL = "https://u.y.qq.com/cgi-bin/musicu.fcg";
const WEB_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const MOBILE_UA = "QQMusic 14090008(android 15)";

export const hash33 = (value, seed = 0) => {
  let hash = seed;
  for (const character of value) hash = (((hash << 5) + hash + character.charCodeAt(0)) & 0xffffffff) >>> 0;
  return hash & 2147483647;
};

const commonParams = () => ({
  ct: 11, cv: 14090008, v: 14090008, chid: "10003505", os_ver: "15",
  phonetype: "24122RKC7C", tmeAppID: "qqmusic", nettype: "NETWORK_WIFI",
  udid: "0", OpenUDID: "0", QIMEI36: "0", uin: "0",
});

const responseCookies = (response, existing = {}) => {
  const result = { ...existing };
  const values = response.headers.getSetCookie?.() ?? [];
  const rawValues = values.length ? values : (response.headers.get("set-cookie")?.split(/,\s*(?=[A-Za-z0-9_-]+=)/) ?? []);
  for (const raw of rawValues) {
    const pair = raw.split(";", 1)[0];
    const index = pair.indexOf("=");
    if (index > 0) result[pair.slice(0, index).trim()] = pair.slice(index + 1).trim();
  }
  return result;
};

export const qqMusicRequest = async (cookies, module, method, param, options = {}) => {
  const accountId = getQQAccountId(cookies);
  const musicKey = cookies.qm_keyst || cookies.qqmusic_key || "";
  const loginType = options.loginType || Number(cookies.tmeLoginType) || (musicKey.startsWith("W_X") ? 1 : 2);
  const response = await fetch(QM_API_URL, {
    method: "POST",
    headers: {
      Accept: "application/json", "Content-Type": "application/json", "User-Agent": MOBILE_UA,
      Referer: "https://y.qq.com", ...(cookieObjectToHeader(cookies) ? { Cookie: cookieObjectToHeader(cookies) } : {}),
    },
    body: JSON.stringify({
      comm: {
        ...commonParams(),
        ...(accountId ? { uin: accountId, qq: accountId } : {}),
        ...(musicKey ? { authst: musicKey, tmeLoginType: loginType } : {}),
        ...(options.loginType ? { tmeLoginType: options.loginType } : {}),
      },
      request: { module, method, param },
    }),
    signal: AbortSignal.timeout(options.timeout ?? 12_000),
  });
  const raw = await response.text();
  if (!response.ok) throw new Error(`QQ 音乐请求失败: HTTP ${response.status}${raw ? ` (${raw.replace(/\s+/g, " ").slice(0, 120)})` : ""}`);
  let payload;
  try { payload = JSON.parse(raw); }
  catch { throw new Error(`QQ 音乐返回非 JSON: ${raw.replace(/\s+/g, " ").slice(0, 120)}`); }
  if (Number(payload.code ?? 0) !== 0 || Number(payload.request?.code ?? 0) !== 0) {
    throw new Error(`QQ 音乐请求失败: outer=${payload.code ?? 0}, inner=${payload.request?.code ?? -1}`);
  }
  return payload.request?.data ?? {};
};

export const credentialToSession = (credential, loginType, fallbackAccountId = "") => {
  const accountId = String(credential.str_musicid || credential.musicid || fallbackAccountId).replace(/^o/, "");
  const session = {
    uin: accountId,
    qm_str_musicid: accountId,
    qm_keyst: credential.musickey || "",
    qqmusic_key: credential.musickey || "",
    tmeLoginType: String(credential.loginType || loginType),
  };
  if (loginType === 1) session.wxuin = accountId;
  if (credential.encryptUin) session.euin = credential.encryptUin;
  if (credential.openid) session[loginType === 1 ? "wxopenid" : "psrf_qqopenid"] = credential.openid;
  if (credential.unionid) session.psrf_qqunionid = credential.unionid;
  if (credential.refresh_token) session[loginType === 1 ? "wxrefresh_token" : "psrf_qqrefresh_token"] = credential.refresh_token;
  if (credential.access_token) session.psrf_qqaccess_token = credential.access_token;
  if (credential.refresh_key) session.qm_refresh_key = credential.refresh_key;
  if (credential.expired_at) session.psrf_access_token_expiresAt = String(credential.expired_at);
  if (credential.musickeyCreateTime) session.psrf_musickey_createtime = String(credential.musickeyCreateTime);
  if (credential.keyExpiresIn) session.qm_key_expires_in = String(credential.keyExpiresIn);
  return session;
};

export const createQrLogin = async (type = "qq") => {
  if (type === "wx") {
    const params = new URLSearchParams({
      appid: "wx48db31d50e334801",
      redirect_uri: "https://y.qq.com/portal/wx_redirect.html?login_type=2&surl=https://y.qq.com/",
      response_type: "code", scope: "snsapi_login", state: "STATE",
      href: "https://y.qq.com/mediastyle/music_v17/src/css/popup_wechat.css#wechat_redirect",
    });
    const page = await fetch(`https://open.weixin.qq.com/connect/qrconnect?${params}`, {
      headers: { "User-Agent": WEB_UA }, signal: AbortSignal.timeout(10_000),
    });
    if (!page.ok) throw new Error(`获取微信二维码失败: HTTP ${page.status}`);
    const html = await page.text();
    // 2026-09-28 修复：旧正则 /(?:QRLogin\.uuid|uuid)\s*[=:]\s*["']([^"']+)["']/i
    // 会误命中页面里的 **JS 模板拼接串** —— 形如
    //   url: U + "/connect/l/qrconnect?uuid=" + G + (e ? "&last=" + e : "")
    // 其中 `uuid="` 之后紧跟 `+G+(e?`，被 [^"']+ 当作“带引号的 uuid”整个吃掉，
    // 取出垃圾值 "+G+(e?"。由于下方是 `m1 || m2` 短路匹配，垃圾值会盖过第二个
    // 正则给出的**真实** uuid；再拿垃圾去 /connect/qrcode/<垃圾> 只会换回一张
    // HTML 错误页，被 full.py 误判成「微信 AppID 已失效」（实际 AppID 完全有效）。
    //
    // 现在只认「uuid= 后紧跟一段合法 uuid 字符」这一种形态，并强制最小长度 16，
    // 从而既排除 `+G+(e?` 这类模板碎片，也兼容 `uuid=xxx` / `uuid="xxx"` /
    // `uuid: "xxx"` 等历史写法。
    const uuid = html.match(/uuid\s*[=:]\s*["']?([A-Za-z0-9_-]{16,})["']?/i)?.[1];
    if (!uuid) throw new Error("微信登录页面没有返回二维码 UUID");
    const image = await fetch(`https://open.weixin.qq.com/connect/qrcode/${uuid}`, {
      headers: { Referer: "https://open.weixin.qq.com/connect/qrconnect", "User-Agent": WEB_UA },
      signal: AbortSignal.timeout(10_000),
    });
    if (!image.ok) throw new Error(`获取微信二维码图片失败: HTTP ${image.status}`);
    return { type: "wx", key: uuid, content: `data:image/jpeg;base64,${Buffer.from(await image.arrayBuffer()).toString("base64")}` };
  }

  const response = await fetch(`https://ssl.ptlogin2.qq.com/ptqrshow?appid=716027609&e=2&l=M&s=3&d=72&v=4&t=${Math.random()}&daid=383&pt_3rd_aid=100497308`, {
    headers: { Referer: "https://xui.ptlogin2.qq.com/", "User-Agent": WEB_UA }, signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error(`获取 QQ 二维码失败: HTTP ${response.status}`);
  const key = responseCookies(response).qrsig;
  if (!key) throw new Error("QQ 登录没有返回 qrsig");
  return { type: "qq", key, content: `data:image/png;base64,${Buffer.from(await response.arrayBuffer()).toString("base64")}` };
};

const profileFromCredential = (credential, accountId) => ({
  userId: accountId,
  nickname: credential.nick || credential.nickname || "",
  avatarUrl: credential.logo || credential.avatarUrl || `https://q.qlogo.cn/headimg_dl?dst_uin=${accountId}&spec=100`,
});

const finishWeChatLogin = async (code) => {
  const credential = await qqMusicRequest({}, "music.login.LoginServer", "Login", {
    code, strAppid: "wx48db31d50e334801",
  }, { loginType: 1 });
  const cookies = credentialToSession(credential, 1);
  const accountId = getQQAccountId(cookies);
  if (!accountId || !credential.musickey) throw new Error("微信登录响应缺少 QQ 音乐凭据");
  return { cookies, profile: profileFromCredential(credential, accountId) };
};

const finishQQLogin = async (key, jumpUrl, loginResponse) => {
  const initialCookies = responseCookies(loginResponse, { qrsig: key });
  const checkSignature = await fetch(jumpUrl, {
    headers: { Referer: "https://xui.ptlogin2.qq.com/", Cookie: cookieObjectToHeader(initialCookies), "User-Agent": WEB_UA },
    redirect: "manual", signal: AbortSignal.timeout(10_000),
  });
  const sessionCookies = responseCookies(checkSignature, initialCookies);
  const skey = sessionCookies.p_skey || sessionCookies.p_sKey || sessionCookies.skey || sessionCookies.pskey;
  if (!skey) throw new Error("QQ 授权没有返回 p_skey");
  const authBody = new URLSearchParams({
    response_type: "code", client_id: "100497308",
    redirect_uri: "https://y.qq.com/portal/wx_redirect.html?login_type=1&surl=https://y.qq.com/",
    scope: "get_user_info,get_app_friends", state: "state", switch: "", from_ptlogin: "1", src: "1",
    update_auth: "1", openapi: "1010_1030", g_tk: String(hash33(skey, 5381)),
    auth_time: String(Date.now()), ui: randomUUID(),
  });
  const authorization = await fetch("https://graph.qq.com/oauth2.0/authorize", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded", Referer: "https://xui.ptlogin2.qq.com/", Cookie: cookieObjectToHeader(sessionCookies), "User-Agent": WEB_UA },
    body: authBody.toString(), redirect: "manual", signal: AbortSignal.timeout(10_000),
  });
  const location = authorization.headers.get("location") || "";
  let code = "";
  try { code = new URL(location).searchParams.get("code") || ""; } catch {}
  if (!code) {
    const body = await authorization.text();
    code = body.match(/[?&]code=([^&'"\s]+)/i)?.[1] || "";
  }
  if (!code) throw new Error("QQ 授权没有返回 code");
  const credential = await qqMusicRequest({}, "QQConnectLogin.LoginServer", "QQLogin", { code }, { loginType: 2 });
  const fallbackAccountId = new URL(jumpUrl).searchParams.get("uin") || "";
  const cookies = credentialToSession(credential, 2, fallbackAccountId);
  const accountId = getQQAccountId(cookies);
  if (!accountId || !credential.musickey) throw new Error("QQ 登录响应缺少 QQ 音乐凭据");
  return { cookies, profile: profileFromCredential(credential, accountId) };
};

export const checkQrLogin = async (type, key) => {
  if (!key) throw new Error("二维码 key 不能为空");
  if (type === "wx") {
    const query = new URLSearchParams({ uuid: key, _: String(Date.now()) });
    const response = await fetch(`https://lp.open.weixin.qq.com/connect/l/qrconnect?${query}`, {
      headers: { Referer: "https://open.weixin.qq.com/", "User-Agent": WEB_UA }, signal: AbortSignal.timeout(35_000),
    });
    const body = await response.text();
    const match = body.match(/window\.wx_errcode\s*=\s*["']?(\d+)["']?\s*;\s*window\.wx_code\s*=\s*["']([^"']*)["']/i);
    if (!match) return { state: "waiting" };
    if (match[1] === "404") return { state: "scanned" };
    if (["402", "403"].includes(match[1])) return { state: "expired" };
    if (match[1] === "405" && match[2]) return { state: "success", ...(await finishWeChatLogin(match[2])) };
    return { state: "waiting" };
  }

  const query = new URLSearchParams({
    u1: "https://graph.qq.com/oauth2.0/login_jump", ptqrtoken: String(hash33(key)), ptredirect: "0",
    h: "1", t: "1", g: "1", from_ui: "1", ptlang: "2052", action: `0-0-${Date.now()}`,
    js_ver: "20102616", js_type: "1", pt_uistyle: "40", aid: "716027609", daid: "383",
    pt_3rd_aid: "100497308", has_onekey: "1",
  });
  const response = await fetch(`https://ssl.ptlogin2.qq.com/ptqrlogin?${query}`, {
    headers: { Referer: "https://xui.ptlogin2.qq.com/", Cookie: `qrsig=${key};`, "User-Agent": WEB_UA },
    signal: AbortSignal.timeout(10_000),
  });
  const callback = (await response.text()).match(/ptuiCB\((.*?)\)/)?.[1];
  if (!callback) return { state: "waiting" };
  const args = [...callback.matchAll(/'((?:\\.|[^'])*)'/g)].map((match) => match[1]);
  if (args[0] === "65") return { state: "expired" };
  if (args[0] === "67") return { state: "scanned", nickname: args[5] || "" };
  if (args[0] === "0") {
    if (!args[2]?.startsWith("http")) throw new Error("QQ 登录跳转地址无效");
    return { state: "success", ...(await finishQQLogin(key, args[2], response)) };
  }
  return { state: "waiting" };
};

export const fetchQQProfile = async (cookies) => {
  const accountId = getQQAccountId(cookies);
  if (!accountId) throw new Error("QQ 音乐尚未登录");
  const data = await qqMusicRequest(cookies, "music.UserInfo.userInfoServer", "GetLoginUserInfo", {});
  const info = data.info ?? {};
  return {
    userId: accountId,
    nickname: info.nick || info.nickname || info.name || "",
    avatarUrl: String(info.logo || "").replace(/^http:/, "https:"),
  };
};
