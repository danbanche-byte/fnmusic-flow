import { chmod, mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";

export const stateDir = process.env.STATE_DIR || "/data";
export const paths = {
  settings: process.env.RUNTIME_SETTINGS_FILE || join(stateDir, "settings.json"),
  qqSession: process.env.QQ_SESSION_FILE || join(stateDir, "qq-session.json"),
  qqCookie: process.env.QQ_MUSIC_COOKIE_FILE || join(stateDir, "qq_music_cookie.txt"),
  fnosToken: process.env.FNOS_MUSIC_TOKEN_FILE || join(stateDir, "fnos_music_token.txt"),
  ttMusicToken: process.env.TTMUSIC_USER_TOKEN_FILE || join(stateDir, "ttmusic_user_token.txt"),
  syncState: join(stateDir, "state-v3.json"),
};

export const readJson = async (file, fallback = null) => {
  try { return JSON.parse(await readFile(file, "utf8")); }
  catch (error) {
    if (error?.code === "ENOENT") return fallback;
    throw error;
  }
};

export const readText = async (file, fallback = "") => {
  try { return (await readFile(file, "utf8")).trim(); }
  catch (error) {
    if (error?.code === "ENOENT") return fallback;
    throw error;
  }
};

export const writePrivateFile = async (file, value) => {
  await mkdir(dirname(file), { recursive: true });
  const temporary = `${file}.${process.pid}.tmp`;
  const handle = await open(temporary, "w", 0o600);
  try {
    await handle.writeFile(value, "utf8");
    await handle.sync();
  } finally { await handle.close(); }
  await rename(temporary, file);
  await chmod(file, 0o600).catch(() => {});
};

export const writePrivateJson = (file, value) => writePrivateFile(file, `${JSON.stringify(value, null, 2)}\n`);

export const removePrivateFile = async (file) => {
  await unlink(file).catch((error) => { if (error?.code !== "ENOENT") throw error; });
};

export const cookieObjectToHeader = (cookies = {}) => Object.entries(cookies)
  .filter(([, value]) => value)
  .map(([name, value]) => `${name}=${value}`)
  .join("; ");

export const parseCookieHeader = (header = "") => Object.fromEntries(header.split(";").map((part) => {
  const index = part.indexOf("=");
  return index > 0 ? [part.slice(0, index).trim(), part.slice(index + 1).trim()] : ["", ""];
}).filter(([name, value]) => name && value));

export const getQQAccountId = (cookies = {}) => String(
  cookies.qm_str_musicid || cookies.uin || cookies.wxuin || cookies.p_uin || "",
).replace(/^o/, "").replace(/^0+(?=\d)/, "");

export const saveQQSession = async (cookies, profile = {}) => {
  const accountId = getQQAccountId(cookies);
  if (!accountId || !(cookies.qm_keyst || cookies.qqmusic_key)) throw new Error("QQ 登录凭据不完整");
  const session = { version: 1, accountId, profile, cookies, updatedAt: new Date().toISOString() };
  await writePrivateJson(paths.qqSession, session);
  await writePrivateFile(paths.qqCookie, `${cookieObjectToHeader(cookies)}\n`);
  return session;
};

export const loadQQSession = async () => {
  const session = await readJson(paths.qqSession, null);
  if (session?.version === 1 && session.cookies && getQQAccountId(session.cookies)) return session;
  const legacy = parseCookieHeader(await readText(paths.qqCookie, ""));
  if (getQQAccountId(legacy) && (legacy.qm_keyst || legacy.qqmusic_key)) {
    return { version: 1, accountId: getQQAccountId(legacy), profile: {}, cookies: legacy, updatedAt: null };
  }
  return null;
};

export const clearQQSession = async () => {
  await Promise.all([removePrivateFile(paths.qqSession), removePrivateFile(paths.qqCookie)]);
};
