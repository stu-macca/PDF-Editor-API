import { createClient } from '@supabase/supabase-js';

const DEFAULT_PRODUCT_CODE = 'droptext-pdf';
const DEFAULT_DEVICE_LIMIT = 2;
const DEFAULT_OFFLINE_GRACE_DAYS = 7;
const DEFAULT_DEVICE_NAME = 'Unknown device';
const LEMONSQUEEZY_ORDER_CREATED_EVENT = 'order_created';
const LEMONSQUEEZY_ORDER_REFUNDED_EVENT = 'order_refunded';
const LICENCE_PLAN_BASIC = 'basic';
const LICENCE_STATUS_ACTIVE = 'active';
const LICENCE_STATUS_CANCELLED = 'cancelled';
const AUTH_USERS_PAGE_SIZE = 200;
const MAX_AUTH_USER_SEARCH_PAGES = 25;
const DEFAULT_PRODUCT_CONFIG = {
  [DEFAULT_PRODUCT_CODE]: {
    device_limit: DEFAULT_DEVICE_LIMIT,
    offline_grace_days: DEFAULT_OFFLINE_GRACE_DAYS,
  },
  'submissions-pdf': {
    device_limit: DEFAULT_DEVICE_LIMIT,
    offline_grace_days: DEFAULT_OFFLINE_GRACE_DAYS,
  },
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'content-type': 'application/json',
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': 'content-type, authorization',
    },
  });
}

function getBearerToken(request) {
  const auth = request.headers.get('authorization') || '';
  if (!auth.startsWith('Bearer ')) return null;
  return auth.slice('Bearer '.length).trim();
}

function getRequestContext(request) {
  const url = new URL(request.url);
  return {
    path: url.pathname,
    method: request.method,
    requestId:
      request.headers.get('cf-ray') ||
      request.headers.get('x-request-id') ||
      crypto.randomUUID(),
  };
}

function logValidate(event, details) {
  console.log(JSON.stringify({ scope: 'licensing.validate', event, ...details }));
}

function logValidateError(event, details) {
  console.error(JSON.stringify({ scope: 'licensing.validate', event, ...details }));
}

function logWebhook(event, details) {
  console.log(JSON.stringify({ scope: 'licensing.webhook', event, ...details }));
}

function logWebhookError(event, details) {
  console.error(JSON.stringify({ scope: 'licensing.webhook', event, ...details }));
}

function logSubmissionsDiagnostic(event, details) {
  console.log(
    JSON.stringify({ scope: 'licensing.submissions_diagnostic', event, ...details }),
  );
}

function normalizeString(value) {
  if (typeof value !== 'string') return null;
  const normalized = value.trim();
  return normalized || null;
}

function normalizeEmail(value) {
  const normalized = normalizeString(value);
  return normalized ? normalized.toLowerCase() : null;
}

function normalizeProductCode(value) {
  const normalized = normalizeString(value);
  return normalized ? normalized.toLowerCase() : null;
}

function normalizeIdentifier(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }

  return normalizeString(value);
}

function readBodyValue(body, snakeCaseKey, camelCaseKey) {
  return body?.[snakeCaseKey] ?? body?.[camelCaseKey] ?? null;
}

function describeDeviceHash(deviceHash) {
  if (!deviceHash) return null;
  if (deviceHash.length <= 12) return deviceHash;
  return `${deviceHash.slice(0, 6)}...${deviceHash.slice(-4)}`;
}

function isExpired(expiresAt) {
  if (!expiresAt) return false;
  const timestamp = Date.parse(expiresAt);
  return Number.isFinite(timestamp) && timestamp <= Date.now();
}

function parseJsonEnv(value, name, fallback = {}) {
  if (!value) return fallback;

  let parsed;
  try {
    parsed = JSON.parse(value);
  } catch (error) {
    throw new Error(`Invalid ${name}: ${error.message}`);
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`Invalid ${name}: expected JSON object`);
  }

  return parsed;
}

function getProductSettingsMap(env) {
  const configured = parseJsonEnv(
    env.PRODUCT_CONFIG_JSON,
    'PRODUCT_CONFIG_JSON',
    {},
  );
  const merged = { ...DEFAULT_PRODUCT_CONFIG };

  for (const [rawProductCode, rawSettings] of Object.entries(configured)) {
    const productCode = normalizeProductCode(rawProductCode);
    if (!productCode || !rawSettings || typeof rawSettings !== 'object') {
      continue;
    }

    const deviceLimit = Number(rawSettings.device_limit);
    const offlineGraceDays = Number(rawSettings.offline_grace_days);

    merged[productCode] = {
      device_limit:
        Number.isFinite(deviceLimit) && deviceLimit > 0
          ? deviceLimit
          : DEFAULT_DEVICE_LIMIT,
      offline_grace_days:
        Number.isFinite(offlineGraceDays) && offlineGraceDays >= 0
          ? offlineGraceDays
          : DEFAULT_OFFLINE_GRACE_DAYS,
    };
  }

  return merged;
}

function getVariantProductMap(env, productSettingsMap) {
  const configured = parseJsonEnv(
    env.LEMONSQUEEZY_VARIANT_PRODUCT_MAP,
    'LEMONSQUEEZY_VARIANT_PRODUCT_MAP',
    {},
  );
  const mapping = {};

  for (const [rawVariantId, rawValue] of Object.entries(configured)) {
    const variantId = normalizeIdentifier(rawVariantId);
    if (!variantId) continue;

    let productCode = null;
    let plan = LICENCE_PLAN_BASIC;

    if (typeof rawValue === 'string') {
      productCode = normalizeProductCode(rawValue);
    } else if (rawValue && typeof rawValue === 'object') {
      productCode = normalizeProductCode(
        rawValue.product_code || rawValue.productCode,
      );
      plan = normalizeString(rawValue.plan) || LICENCE_PLAN_BASIC;
    }

    if (!productCode || !productSettingsMap[productCode]) {
      continue;
    }

    mapping[variantId] = {
      productCode,
      plan,
    };
  }

  return mapping;
}

function resolveRequestedProduct(body, productSettingsMap, { allowDefault = false } = {}) {
  const requestedProductCode = normalizeProductCode(
    readBodyValue(body, 'product_code', 'productCode'),
  );
  const productCode =
    requestedProductCode || (allowDefault ? DEFAULT_PRODUCT_CODE : null);

  if (!productCode) {
    return { error: 'missing_product_code' };
  }

  const settings = productSettingsMap[productCode];
  if (!settings) {
    return { error: 'invalid_product_code' };
  }

  return { productCode, settings };
}

function normalizeStoredProductCode(value) {
  return normalizeProductCode(value) || DEFAULT_PRODUCT_CODE;
}

function createdTimestamp(value) {
  const timestamp = Date.parse(value || '');
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function compareLicences(a, b) {
  const aPriority =
    a.status === LICENCE_STATUS_ACTIVE && !isExpired(a.expires_at)
      ? 2
      : a.status === LICENCE_STATUS_ACTIVE
        ? 1
        : 0;
  const bPriority =
    b.status === LICENCE_STATUS_ACTIVE && !isExpired(b.expires_at)
      ? 2
      : b.status === LICENCE_STATUS_ACTIVE
        ? 1
        : 0;

  if (aPriority !== bPriority) {
    return bPriority - aPriority;
  }

  return createdTimestamp(b.created_at) - createdTimestamp(a.created_at);
}

function compareByCreatedAtDesc(a, b) {
  return createdTimestamp(b.created_at) - createdTimestamp(a.created_at);
}

function selectPreferredLicence(licences, productCode) {
  const matchingLicences = licences
    .filter((licence) => normalizeStoredProductCode(licence.product_code) === productCode)
    .sort(compareLicences);

  return matchingLicences[0] || null;
}

function selectLatestLicence(licences, productCode) {
  const matchingLicences = licences
    .filter((licence) => normalizeStoredProductCode(licence.product_code) === productCode)
    .sort(compareByCreatedAtDesc);

  return matchingLicences[0] || null;
}

function serializeEntitlement(licence, productCode) {
  if (!licence) return null;

  return {
    product_code: productCode || normalizeStoredProductCode(licence.product_code),
    plan: licence.plan || null,
    status: licence.status || null,
    expires_at: licence.expires_at || null,
  };
}

function listEntitlements(licences) {
  const productCodes = new Set(
    licences.map((licence) => normalizeStoredProductCode(licence.product_code)),
  );

  return [...productCodes]
    .sort()
    .map((productCode) =>
      serializeEntitlement(selectPreferredLicence(licences, productCode), productCode),
    )
    .filter(Boolean);
}

function serializeSession(session) {
  if (!session) return null;

  return {
    access_token: session.access_token,
    refresh_token: session.refresh_token,
    expires_in: session.expires_in,
    expires_at: session.expires_at,
    token_type: session.token_type,
  };
}

function evaluateEntitlement(licences, productCode, noEntitlementReason) {
  const sameProductLicences = licences.filter(
    (licence) => normalizeStoredProductCode(licence.product_code) === productCode,
  );
  const activeProductLicence = sameProductLicences
    .filter((licence) => licence.status === LICENCE_STATUS_ACTIVE)
    .sort(compareLicences)[0];

  if (!activeProductLicence) {
    const selectedSameProductLicence = sameProductLicences.sort(compareByCreatedAtDesc)[0];

    if (!selectedSameProductLicence) {
      return {
        valid: false,
        status: 403,
        reason: noEntitlementReason,
      };
    }

    return {
      valid: false,
      status: 403,
      reason: 'licence_inactive',
      licence: selectedSameProductLicence,
    };
  }

  if (isExpired(activeProductLicence.expires_at)) {
    return {
      valid: false,
      status: 403,
      reason: 'expired',
      licence: activeProductLicence,
    };
  }

  return {
    valid: true,
    licence: activeProductLicence,
  };
}

function filterDevicesForProduct(devices, productCode) {
  return devices.filter(
    (device) => normalizeStoredProductCode(device.product_code) === productCode,
  );
}

function createAdminClient(env) {
  if (!env.SUPABASE_URL || !env.SUPABASE_SECRET_KEY) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_SECRET_KEY');
  }

  return createClient(env.SUPABASE_URL, env.SUPABASE_SECRET_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
}

function createAuthClient(env) {
  const key = env.SUPABASE_ANON_KEY || env.SUPABASE_SECRET_KEY;
  if (!env.SUPABASE_URL || !key) {
    throw new Error(
      'Missing SUPABASE_URL and either SUPABASE_ANON_KEY or SUPABASE_SECRET_KEY',
    );
  }

  return createClient(env.SUPABASE_URL, key, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
  });
}

async function readJsonBody(request) {
  const body = await request.json().catch(() => ({}));
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return {};
  }
  return body;
}

function invalid(reason, status, context) {
  logValidate('reject', { ...context, reason });
  return json({ valid: false, reason }, status);
}

function invalidV1(reason, status = 400) {
  return json({ ok: false, reason }, status);
}

function authError(error, status = 401) {
  return json({ ok: false, error }, status);
}

function toHex(buffer) {
  return Array.from(new Uint8Array(buffer), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

function timingSafeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) {
    return false;
  }

  let mismatch = 0;
  for (let i = 0; i < a.length; i += 1) {
    mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }

  return mismatch === 0;
}

async function createHmacSha256Hex(secret, payload) {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );

  const digest = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(payload),
  );

  return toHex(digest);
}

async function verifyLemonSqueezySignature(rawBody, signature, secret) {
  if (!rawBody || !signature || !secret) return false;
  const expectedSignature = await createHmacSha256Hex(secret, rawBody);
  return timingSafeEqual(expectedSignature, signature.toLowerCase());
}

async function findAuthUserByEmail(admin, email) {
  for (let page = 1; page <= MAX_AUTH_USER_SEARCH_PAGES; page += 1) {
    const { data, error } = await admin.auth.admin.listUsers({
      page,
      perPage: AUTH_USERS_PAGE_SIZE,
    });

    if (error) {
      throw new Error(`Auth user lookup failed: ${error.message}`);
    }

    const users = data?.users || [];
    const matchedUser = users.find(
      (user) => normalizeEmail(user.email) === email,
    );

    if (matchedUser) {
      return matchedUser;
    }

    if (users.length < AUTH_USERS_PAGE_SIZE) {
      return null;
    }
  }

  throw new Error('Auth user lookup exceeded pagination limit');
}

async function resolveWebhookUser(admin, email) {
  const { data: profile, error: profileError } = await admin
    .from('profiles')
    .select('id, email')
    .ilike('email', email)
    .limit(1)
    .maybeSingle();

  if (profileError) {
    throw new Error(`Profile lookup failed: ${profileError.message}`);
  }

  if (profile?.id) {
    return { userId: profile.id, userSource: 'profile' };
  }

  const existingAuthUser = await findAuthUserByEmail(admin, email);
  if (existingAuthUser?.id) {
    return { userId: existingAuthUser.id, userSource: 'auth' };
  }

  const { data: createdUserData, error: createUserError } =
    await admin.auth.admin.createUser({
      email,
      email_confirm: true,
    });

  if (createUserError) {
    const fallbackUser = await findAuthUserByEmail(admin, email);
    if (fallbackUser?.id) {
      return { userId: fallbackUser.id, userSource: 'auth' };
    }

    throw new Error(`Auth user creation failed: ${createUserError.message}`);
  }

  const createdUser = createdUserData?.user;
  if (!createdUser?.id) {
    throw new Error('Auth user creation returned no user');
  }

  return { userId: createdUser.id, userSource: 'created' };
}

async function syncProfile(admin, userId, email, context, logFn) {
  if (!userId || !email) return;

  const { error } = await admin.from('profiles').upsert({
    id: userId,
    email,
  });

  if (error && logFn) {
    logFn('profile_sync_failed', {
      ...context,
      userId,
      message: error.message,
    });
  }
}

async function loadUserLicences(admin, userId) {
  const { data: licences = [], error } = await admin
    .from('licences')
    .select('*')
    .eq('user_id', userId);

  if (error) {
    throw new Error(`Licence lookup failed: ${error.message}`);
  }

  return licences;
}

async function loadLicencesByOrderId(admin, orderId) {
  const { data: licences = [], error } = await admin
    .from('licences')
    .select('*')
    .eq('lemonsqueezy_order_id', orderId);

  if (error) {
    throw new Error(`Order-linked licence lookup failed: ${error.message}`);
  }

  return licences.sort(compareByCreatedAtDesc);
}

async function loadUserDevices(admin, userId) {
  const { data: devices = [], error } = await admin
    .from('devices')
    .select('*')
    .eq('user_id', userId);

  if (error) {
    throw new Error(`Device lookup failed: ${error.message}`);
  }

  return devices;
}

async function activateLicence(admin, userId, activation) {
  const existingLicences = await loadUserLicences(admin, userId);
  const existingLicence = selectLatestLicence(
    existingLicences,
    activation.productCode,
  );
  const licencePayload = {
    user_id: userId,
    product_code: activation.productCode,
    plan: activation.plan || LICENCE_PLAN_BASIC,
    status: LICENCE_STATUS_ACTIVE,
    expires_at: null,
    lemonsqueezy_order_id: activation.orderId,
    lemonsqueezy_product_id: activation.productId,
    lemonsqueezy_variant_id: activation.variantId,
  };

  if (existingLicence?.id) {
    const { error: licenceUpdateError } = await admin
      .from('licences')
      .update(licencePayload)
      .eq('id', existingLicence.id);

    if (licenceUpdateError) {
      throw new Error(`Licence update failed: ${licenceUpdateError.message}`);
    }

    return {
      action: 'updated',
      licenceId: existingLicence.id,
      previousPlan: existingLicence.plan || null,
      previousStatus: existingLicence.status || null,
      productCode: activation.productCode,
    };
  }

  const { error: licenceInsertError } = await admin
    .from('licences')
    .insert(licencePayload);

  if (licenceInsertError) {
    throw new Error(`Licence create failed: ${licenceInsertError.message}`);
  }

  return {
    action: 'created',
    licenceId: null,
    previousPlan: null,
    previousStatus: null,
    productCode: activation.productCode,
  };
}

async function cancelLicencesByOrderId(admin, orderId) {
  const matchingLicences = await loadLicencesByOrderId(admin, orderId);

  if (!matchingLicences.length) {
    return {
      action: 'not_found',
      matchedCount: 0,
      matchedLicenceIds: [],
      cancelledLicenceIds: [],
      alreadyCancelledLicenceIds: [],
      safeBackfillAvailable: false,
    };
  }

  const cancelledLicenceIds = [];
  const alreadyCancelledLicenceIds = [];

  for (const licence of matchingLicences) {
    const licenceStatus = normalizeString(licence.status)?.toLowerCase();

    if (licenceStatus === LICENCE_STATUS_CANCELLED) {
      alreadyCancelledLicenceIds.push(licence.id);
      continue;
    }

    const { error: licenceUpdateError } = await admin
      .from('licences')
      .update({ status: LICENCE_STATUS_CANCELLED })
      .eq('id', licence.id);

    if (licenceUpdateError) {
      throw new Error(`Licence cancellation failed: ${licenceUpdateError.message}`);
    }

    cancelledLicenceIds.push(licence.id);
  }

  return {
    action: cancelledLicenceIds.length ? 'cancelled' : 'already_cancelled',
    matchedCount: matchingLicences.length,
    matchedLicenceIds: matchingLicences.map((licence) => licence.id),
    cancelledLicenceIds,
    alreadyCancelledLicenceIds,
    safeBackfillAvailable: false,
  };
}

async function getAuthenticatedUser(admin, token) {
  const { data: userData, error: userError } = await admin.auth.getUser(token);

  if (userError || !userData?.user) {
    return {
      error: 'invalid_session',
      status: 401,
      message: userError?.message || 'No user returned from Supabase auth',
    };
  }

  return {
    user: userData.user,
  };
}

function buildAccount(user) {
  return {
    id: user.id,
    email: user.email,
  };
}

async function buildAccountResponseData(admin, user) {
  const licences = await loadUserLicences(admin, user.id);
  return {
    account: buildAccount(user),
    entitlements: listEntitlements(licences),
  };
}

async function registerOrRefreshDevice(admin, params) {
  const existingDevices = filterDevicesForProduct(params.devices, params.productCode);
  const matchedDevice = existingDevices.find(
    (device) => device.device_hash === params.deviceHash,
  );
  const now = new Date().toISOString();

  if (!matchedDevice) {
    if (existingDevices.length >= params.deviceLimit) {
      return {
        ok: false,
        reason: 'device_limit',
        status: 403,
        deviceCount: existingDevices.length,
      };
    }

    const { error: insertError } = await admin.from('devices').insert({
      user_id: params.userId,
      product_code: params.productCode,
      device_hash: params.deviceHash,
      device_name: params.deviceName,
      last_seen_at: now,
    });

    if (insertError) {
      throw new Error(`Device insert failed: ${insertError.message}`);
    }

    return {
      ok: true,
      action: 'registered',
      count: existingDevices.length + 1,
    };
  }

  const { error: updateError } = await admin
    .from('devices')
    .update({
      product_code: params.productCode,
      device_name: params.deviceName,
      last_seen_at: now,
    })
    .eq('id', matchedDevice.id);

  if (updateError) {
    throw new Error(`Device update failed: ${updateError.message}`);
  }

  return {
    ok: true,
    action: 'refreshed',
    count: existingDevices.length,
  };
}

async function handleValidate(request, env) {
  const requestContext = getRequestContext(request);
  const token = getBearerToken(request);
  if (!token) {
    return invalid('missing_token', 401, requestContext);
  }

  let admin;
  let productSettingsMap;
  try {
    admin = createAdminClient(env);
    productSettingsMap = getProductSettingsMap(env);
  } catch (error) {
    logValidateError('config_error', {
      ...requestContext,
      message: error.message,
    });
    return json({ error: 'internal_error' }, 500);
  }

  try {
    const body = await readJsonBody(request);
    const resolvedProduct = resolveRequestedProduct(body, productSettingsMap, {
      allowDefault: true,
    });
    if (resolvedProduct.error) {
      return invalid(resolvedProduct.error, 400, requestContext);
    }

    const deviceHash = normalizeString(readBodyValue(body, 'device_hash', 'deviceHash'));
    const deviceName =
      normalizeString(readBodyValue(body, 'device_name', 'deviceName')) ||
      DEFAULT_DEVICE_NAME;

    logValidate('start', {
      ...requestContext,
      productCode: resolvedProduct.productCode,
      hasDeviceHash: Boolean(deviceHash),
      deviceHash: describeDeviceHash(deviceHash),
    });

    const authenticated = await getAuthenticatedUser(admin, token);
    if (!authenticated.user) {
      return invalid(authenticated.error, authenticated.status, {
        ...requestContext,
        message: authenticated.message,
      });
    }

    const user = authenticated.user;

    await syncProfile(admin, user.id, user.email, requestContext, logValidate);

    const licences = await loadUserLicences(admin, user.id);
    const entitlement = evaluateEntitlement(
      licences,
      resolvedProduct.productCode,
      'no_licence',
    );

    if (!entitlement.valid) {
      return invalid(entitlement.reason, entitlement.status, {
        ...requestContext,
        userId: user.id,
        productCode: resolvedProduct.productCode,
        licenceId: entitlement.licence?.id || null,
        licenceStatus: entitlement.licence?.status || null,
        expiresAt: entitlement.licence?.expires_at || null,
      });
    }

    if (!deviceHash) {
      return invalid('missing_device', 400, {
        ...requestContext,
        userId: user.id,
        productCode: resolvedProduct.productCode,
      });
    }

    const devices = await loadUserDevices(admin, user.id);
    const deviceResult = await registerOrRefreshDevice(admin, {
      devices,
      userId: user.id,
      productCode: resolvedProduct.productCode,
      deviceHash,
      deviceName,
      deviceLimit: resolvedProduct.settings.device_limit,
    });

    if (!deviceResult.ok) {
      return invalid(deviceResult.reason, deviceResult.status, {
        ...requestContext,
        userId: user.id,
        productCode: resolvedProduct.productCode,
        deviceCount: deviceResult.deviceCount,
        deviceLimit: resolvedProduct.settings.device_limit,
        deviceHash: describeDeviceHash(deviceHash),
      });
    }

    logValidate('success', {
      ...requestContext,
      userId: user.id,
      productCode: resolvedProduct.productCode,
      licenceId: entitlement.licence.id || null,
      plan: entitlement.licence.plan,
      deviceAction: deviceResult.action,
      deviceCount: deviceResult.count,
      deviceHash: describeDeviceHash(deviceHash),
    });

    return json({
      valid: true,
      plan: entitlement.licence.plan,
      status: entitlement.licence.status,
      user: buildAccount(user),
      offlineGraceDays: resolvedProduct.settings.offline_grace_days,
    });
  } catch (error) {
    logValidateError('unexpected_error', {
      ...requestContext,
      message: error.message,
    });

    return json({ error: 'internal_error' }, 500);
  }
}

async function handleAuthSignIn(request, env) {
  let authClient;
  let admin;
  try {
    authClient = createAuthClient(env);
    admin = createAdminClient(env);
  } catch (error) {
    return json({ error: 'internal_error' }, 500);
  }

  const body = await readJsonBody(request);
  const email = normalizeEmail(body.email);
  const password = normalizeString(body.password);

  if (!email || !password) {
    return authError('invalid_request', 400);
  }

  const { data, error } = await authClient.auth.signInWithPassword({
    email,
    password,
  });

  if (error || !data?.user || !data?.session) {
    return authError('invalid_credentials', 401);
  }

  await syncProfile(admin, data.user.id, data.user.email || email);
  const accountData = await buildAccountResponseData(admin, {
    id: data.user.id,
    email: data.user.email || email,
  });

  return json({
    ok: true,
    session: serializeSession(data.session),
    account: accountData.account,
    entitlements: accountData.entitlements,
  });
}

async function handleAuthRestore(request, env) {
  let authClient;
  let admin;
  try {
    authClient = createAuthClient(env);
    admin = createAdminClient(env);
  } catch (error) {
    return json({ error: 'internal_error' }, 500);
  }

  const body = await readJsonBody(request);
  const refreshToken = normalizeString(
    readBodyValue(body, 'refresh_token', 'refreshToken'),
  );

  if (!refreshToken) {
    return authError('invalid_request', 400);
  }

  const { data, error } = await authClient.auth.refreshSession({
    refresh_token: refreshToken,
  });

  if (error || !data?.session) {
    return authError('invalid_refresh_token', 401);
  }

  const authenticated = await getAuthenticatedUser(admin, data.session.access_token);
  if (!authenticated.user) {
    return authError('invalid_session', 401);
  }

  await syncProfile(admin, authenticated.user.id, authenticated.user.email);
  const accountData = await buildAccountResponseData(admin, authenticated.user);

  return json({
    ok: true,
    session: serializeSession(data.session),
    account: accountData.account,
    entitlements: accountData.entitlements,
  });
}

async function authenticateV1Request(request, env) {
  const token = getBearerToken(request);
  if (!token) {
    return { response: invalidV1('missing_token', 401) };
  }

  let admin;
  let productSettingsMap;
  try {
    admin = createAdminClient(env);
    productSettingsMap = getProductSettingsMap(env);
  } catch (error) {
    return { response: json({ error: 'internal_error' }, 500) };
  }

  const authenticated = await getAuthenticatedUser(admin, token);
  if (!authenticated.user) {
    return { response: invalidV1(authenticated.error, authenticated.status) };
  }

  return {
    admin,
    user: authenticated.user,
    productSettingsMap,
  };
}

async function handleEntitlementsValidate(request, env) {
  const body = await readJsonBody(request);
  const requestedProductCode = normalizeProductCode(
    readBodyValue(body, 'product_code', 'productCode'),
  );
  const hasAuthorizationHeader = Boolean(request.headers.get('authorization'));

  const logResponse = async (details, response) => {
    const responseBody = await response.clone().json().catch(() => null);
    logSubmissionsDiagnostic('entitlements_validate_response', {
      path: '/v1/entitlements/validate',
      requestedProductCode,
      ...details,
      responseStatus: response.status,
      responseBody,
    });
  };

  logSubmissionsDiagnostic('entitlements_validate_start', {
    path: '/v1/entitlements/validate',
    requestedProductCode,
    hasAuthorizationHeader,
  });

  const authResult = await authenticateV1Request(request, env);
  if (authResult.response) {
    await logResponse(
      {
        failureReason: 'auth_failed',
      },
      authResult.response,
    );
    return authResult.response;
  }

  const resolvedProduct = resolveRequestedProduct(
    body,
    authResult.productSettingsMap,
  );

  if (resolvedProduct.error) {
    const response = invalidV1(resolvedProduct.error, 400);
    await logResponse(
      {
        userId: authResult.user.id,
        email: authResult.user.email,
        resolvedProductCode: null,
        failureReason: resolvedProduct.error,
      },
      response,
    );
    return response;
  }

  logSubmissionsDiagnostic('entitlements_validate_user', {
    path: '/v1/entitlements/validate',
    requestedProductCode,
    resolvedProductCode: resolvedProduct.productCode,
    userId: authResult.user.id,
    email: authResult.user.email,
  });

  await syncProfile(authResult.admin, authResult.user.id, authResult.user.email);

  const licences = await loadUserLicences(authResult.admin, authResult.user.id);
  logSubmissionsDiagnostic('entitlements_validate_licence_rows', {
    path: '/v1/entitlements/validate',
    requestedProductCode,
    resolvedProductCode: resolvedProduct.productCode,
    userId: authResult.user.id,
    email: authResult.user.email,
    licenceRows: licences.map((licence) => ({
      product_code: normalizeStoredProductCode(licence.product_code),
      status: licence.status || null,
      expires_at: licence.expires_at || null,
    })),
  });

  const entitlement = evaluateEntitlement(
    licences,
    resolvedProduct.productCode,
    'no_entitlement',
  );

  const matchedActiveLicence = licences
    .filter(
      (licence) =>
        normalizeStoredProductCode(licence.product_code) ===
          resolvedProduct.productCode &&
        licence.status === LICENCE_STATUS_ACTIVE,
    )
    .sort(compareLicences)[0];

  logSubmissionsDiagnostic('entitlements_validate_match_result', {
    path: '/v1/entitlements/validate',
    requestedProductCode,
    resolvedProductCode: resolvedProduct.productCode,
    userId: authResult.user.id,
    email: authResult.user.email,
    matchingActiveLicenceFound: Boolean(matchedActiveLicence),
    matchedLicenceProductCode: matchedActiveLicence
      ? normalizeStoredProductCode(matchedActiveLicence.product_code)
      : entitlement.licence
        ? normalizeStoredProductCode(entitlement.licence.product_code)
        : null,
    matchedLicenceStatus: matchedActiveLicence?.status || entitlement.licence?.status || null,
    matchedLicenceExpiresAt:
      matchedActiveLicence?.expires_at || entitlement.licence?.expires_at || null,
  });

  logSubmissionsDiagnostic('entitlements_validate_selected_licence', {
    path: '/v1/entitlements/validate',
    requestedProductCode,
    resolvedProductCode: resolvedProduct.productCode,
    userId: authResult.user.id,
    email: authResult.user.email,
    selectedLicenceRow: entitlement.licence
      ? {
          product_code: normalizeStoredProductCode(entitlement.licence.product_code),
          status: entitlement.licence.status || null,
          expires_at: entitlement.licence.expires_at || null,
        }
      : null,
  });

  if (!entitlement.valid) {
    const response = invalidV1(entitlement.reason, entitlement.status);
    await logResponse(
      {
        userId: authResult.user.id,
        email: authResult.user.email,
        resolvedProductCode: resolvedProduct.productCode,
        matchingActiveLicenceFound: Boolean(matchedActiveLicence),
        matchedLicenceProductCode: entitlement.licence
          ? normalizeStoredProductCode(entitlement.licence.product_code)
          : null,
        matchedLicenceStatus: entitlement.licence?.status || null,
        matchedLicenceExpiresAt: entitlement.licence?.expires_at || null,
        failureReason: entitlement.reason,
      },
      response,
    );
    return response;
  }

  logSubmissionsDiagnostic('entitlements_validate_success', {
    path: '/v1/entitlements/validate',
    requestedProductCode,
    resolvedProductCode: resolvedProduct.productCode,
    userId: authResult.user.id,
    email: authResult.user.email,
    matchingActiveLicenceFound: true,
    matchedLicenceProductCode: normalizeStoredProductCode(
      entitlement.licence.product_code,
    ),
    matchedLicenceStatus: entitlement.licence.status,
    matchedLicenceExpiresAt: entitlement.licence.expires_at || null,
  });

  const response = json({
    ok: true,
    valid: true,
    product_code: resolvedProduct.productCode,
    entitlement: serializeEntitlement(
      entitlement.licence,
      resolvedProduct.productCode,
    ),
    entitlements: listEntitlements(licences),
    account: buildAccount(authResult.user),
    offline_grace_days: resolvedProduct.settings.offline_grace_days,
  });
  await logResponse(
    {
      userId: authResult.user.id,
      email: authResult.user.email,
      resolvedProductCode: resolvedProduct.productCode,
      matchingActiveLicenceFound: true,
      matchedLicenceProductCode: normalizeStoredProductCode(
        entitlement.licence.product_code,
      ),
      matchedLicenceStatus: entitlement.licence.status,
      matchedLicenceExpiresAt: entitlement.licence.expires_at || null,
    },
    response,
  );
  return response;
}

async function handleDevicesCheck(request, env) {
  const authResult = await authenticateV1Request(request, env);
  if (authResult.response) {
    return authResult.response;
  }

  const body = await readJsonBody(request);
  const resolvedProduct = resolveRequestedProduct(
    body,
    authResult.productSettingsMap,
  );
  if (resolvedProduct.error) {
    return invalidV1(resolvedProduct.error, 400);
  }

  const deviceHash = normalizeString(readBodyValue(body, 'device_hash', 'deviceHash'));
  if (!deviceHash) {
    return invalidV1('missing_device', 400);
  }

  await syncProfile(authResult.admin, authResult.user.id, authResult.user.email);

  const licences = await loadUserLicences(authResult.admin, authResult.user.id);
  const entitlement = evaluateEntitlement(
    licences,
    resolvedProduct.productCode,
    'no_entitlement',
  );
  if (!entitlement.valid) {
    return invalidV1(entitlement.reason, entitlement.status);
  }

  const devices = await loadUserDevices(authResult.admin, authResult.user.id);
  const productDevices = filterDevicesForProduct(
    devices,
    resolvedProduct.productCode,
  );
  const matchedDevice = productDevices.find(
    (device) => device.device_hash === deviceHash,
  );

  return json({
    ok: true,
    product_code: resolvedProduct.productCode,
    device: {
      registered: Boolean(matchedDevice),
      count: productDevices.length,
      limit: resolvedProduct.settings.device_limit,
      can_register:
        Boolean(matchedDevice) ||
        productDevices.length < resolvedProduct.settings.device_limit,
    },
  });
}

async function handleDevicesRegister(request, env) {
  const body = await readJsonBody(request);
  const requestedProductCode = normalizeProductCode(
    readBodyValue(body, 'product_code', 'productCode'),
  );
  const deviceHash = normalizeString(readBodyValue(body, 'device_hash', 'deviceHash'));
  const shouldLogDiagnostic = requestedProductCode === 'submissions-pdf';

  if (shouldLogDiagnostic) {
    logSubmissionsDiagnostic('devices_register_start', {
      path: '/v1/devices/register',
      hasAuthorizationHeader: Boolean(request.headers.get('authorization')),
      requestedProductCode,
      deviceHash: describeDeviceHash(deviceHash),
    });
  }

  const authResult = await authenticateV1Request(request, env);
  if (authResult.response) {
    if (shouldLogDiagnostic) {
      const responseBody = await authResult.response.clone().json().catch(() => null);
      logSubmissionsDiagnostic('devices_register_failure', {
        path: '/v1/devices/register',
        requestedProductCode,
        deviceHash: describeDeviceHash(deviceHash),
        failureReason: responseBody?.reason || responseBody?.error || 'unknown',
      });
    }
    return authResult.response;
  }

  const resolvedProduct = resolveRequestedProduct(
    body,
    authResult.productSettingsMap,
  );
  if (resolvedProduct.error) {
    if (shouldLogDiagnostic) {
      logSubmissionsDiagnostic('devices_register_failure', {
        path: '/v1/devices/register',
        userId: authResult.user.id,
        email: authResult.user.email,
        requestedProductCode,
        deviceHash: describeDeviceHash(deviceHash),
        failureReason: resolvedProduct.error,
      });
    }
    return invalidV1(resolvedProduct.error, 400);
  }

  const deviceName =
    normalizeString(readBodyValue(body, 'device_name', 'deviceName')) ||
    DEFAULT_DEVICE_NAME;

  if (!deviceHash) {
    if (shouldLogDiagnostic) {
      logSubmissionsDiagnostic('devices_register_failure', {
        path: '/v1/devices/register',
        userId: authResult.user.id,
        email: authResult.user.email,
        requestedProductCode: resolvedProduct.productCode,
        deviceHash: null,
        failureReason: 'missing_device',
      });
    }
    return invalidV1('missing_device', 400);
  }

  if (shouldLogDiagnostic) {
    logSubmissionsDiagnostic('devices_register_user', {
      path: '/v1/devices/register',
      userId: authResult.user.id,
      email: authResult.user.email,
      requestedProductCode: resolvedProduct.productCode,
      deviceHash: describeDeviceHash(deviceHash),
    });
  }

  await syncProfile(authResult.admin, authResult.user.id, authResult.user.email);

  const licences = await loadUserLicences(authResult.admin, authResult.user.id);
  const entitlement = evaluateEntitlement(
    licences,
    resolvedProduct.productCode,
    'no_entitlement',
  );

  if (shouldLogDiagnostic) {
    const matchedActiveLicence = licences
      .filter(
        (licence) =>
          normalizeStoredProductCode(licence.product_code) ===
            resolvedProduct.productCode &&
          licence.status === LICENCE_STATUS_ACTIVE,
      )
      .sort(compareLicences)[0];

    logSubmissionsDiagnostic('devices_register_licence_lookup', {
      path: '/v1/devices/register',
      userId: authResult.user.id,
      email: authResult.user.email,
      requestedProductCode: resolvedProduct.productCode,
      deviceHash: describeDeviceHash(deviceHash),
      matchingActiveLicenceFound: Boolean(matchedActiveLicence),
      matchedLicenceProductCode: matchedActiveLicence
        ? normalizeStoredProductCode(matchedActiveLicence.product_code)
        : entitlement.licence
          ? normalizeStoredProductCode(entitlement.licence.product_code)
          : null,
      matchedLicenceStatus: matchedActiveLicence?.status || entitlement.licence?.status || null,
    });
  }

  if (!entitlement.valid) {
    if (shouldLogDiagnostic) {
      logSubmissionsDiagnostic('devices_register_failure', {
        path: '/v1/devices/register',
        userId: authResult.user.id,
        email: authResult.user.email,
        requestedProductCode: resolvedProduct.productCode,
        deviceHash: describeDeviceHash(deviceHash),
        matchingActiveLicenceFound: false,
        matchedLicenceProductCode: entitlement.licence
          ? normalizeStoredProductCode(entitlement.licence.product_code)
          : null,
        matchedLicenceStatus: entitlement.licence?.status || null,
        failureReason: entitlement.reason,
      });
    }
    return invalidV1(entitlement.reason, entitlement.status);
  }

  const devices = await loadUserDevices(authResult.admin, authResult.user.id);
  const result = await registerOrRefreshDevice(authResult.admin, {
    devices,
    userId: authResult.user.id,
    productCode: resolvedProduct.productCode,
    deviceHash,
    deviceName,
    deviceLimit: resolvedProduct.settings.device_limit,
  });

  if (!result.ok) {
    if (shouldLogDiagnostic) {
      logSubmissionsDiagnostic('devices_register_failure', {
        path: '/v1/devices/register',
        userId: authResult.user.id,
        email: authResult.user.email,
        requestedProductCode: resolvedProduct.productCode,
        deviceHash: describeDeviceHash(deviceHash),
        matchingActiveLicenceFound: true,
        matchedLicenceProductCode: normalizeStoredProductCode(
          entitlement.licence.product_code,
        ),
        matchedLicenceStatus: entitlement.licence.status,
        failureReason: result.reason,
      });
    }
    return invalidV1(result.reason, result.status);
  }

  if (shouldLogDiagnostic) {
    logSubmissionsDiagnostic('devices_register_success', {
      path: '/v1/devices/register',
      userId: authResult.user.id,
      email: authResult.user.email,
      requestedProductCode: resolvedProduct.productCode,
      deviceHash: describeDeviceHash(deviceHash),
      matchingActiveLicenceFound: true,
      matchedLicenceProductCode: normalizeStoredProductCode(
        entitlement.licence.product_code,
      ),
      matchedLicenceStatus: entitlement.licence.status,
    });
  }

  return json({
    ok: true,
    product_code: resolvedProduct.productCode,
    device: {
      status: result.action,
      count: result.count,
      limit: resolvedProduct.settings.device_limit,
    },
  });
}

async function handleLemonSqueezyWebhook(request, env) {
  const requestContext = getRequestContext(request);
  logWebhook('route_matched', requestContext);
  const rawBody = await request.text();
  const signature = normalizeString(request.headers.get('x-signature'))?.toLowerCase();

  if (!env.LEMONSQUEEZY_WEBHOOK_SECRET) {
    logWebhookError('config_error', {
      ...requestContext,
      message: 'Missing LEMONSQUEEZY_WEBHOOK_SECRET',
    });
    return json({ error: 'internal_error' }, 500);
  }

  if (!signature) {
    logWebhookError('signature_missing', requestContext);
    return json({ error: 'invalid_signature' }, 401);
  }

  const isValidSignature = await verifyLemonSqueezySignature(
    rawBody,
    signature,
    env.LEMONSQUEEZY_WEBHOOK_SECRET,
  );

  if (!isValidSignature) {
    logWebhookError('signature_invalid', requestContext);
    return json({ error: 'invalid_signature' }, 401);
  }

  logWebhook('verified', requestContext);

  let payload;
  try {
    payload = rawBody ? JSON.parse(rawBody) : {};
  } catch (error) {
    logWebhookError('payload_invalid', {
      ...requestContext,
      message: error.message,
    });
    return json({ error: 'invalid_payload' }, 400);
  }

  const headerEventName = normalizeString(request.headers.get('x-event-name'));
  const eventName =
    normalizeString(payload?.meta?.event_name) || headerEventName || 'unknown';
  const order = payload?.data?.attributes || {};
  const orderId = normalizeIdentifier(payload?.data?.id);

  logWebhook('received', {
    ...requestContext,
    eventName,
    orderId,
  });

  if (
    eventName !== LEMONSQUEEZY_ORDER_CREATED_EVENT &&
    eventName !== LEMONSQUEEZY_ORDER_REFUNDED_EVENT
  ) {
    logWebhook('ignored', {
      ...requestContext,
      eventName,
      reason: 'event_not_handled',
    });
    return json({ ok: true, ignored: true });
  }

  const orderStatus = normalizeString(order.status)?.toLowerCase();
  const email = normalizeEmail(order.user_email);

  if (eventName === LEMONSQUEEZY_ORDER_CREATED_EVENT) {
    if (orderStatus && orderStatus !== 'paid') {
      logWebhook('ignored', {
        ...requestContext,
        eventName,
        orderId,
        orderStatus,
        reason: 'order_not_paid',
      });
      return json({ ok: true, ignored: true });
    }

    if (!email) {
      logWebhookError('missing_customer_email', {
        ...requestContext,
        eventName,
        orderId,
      });
      return json({ error: 'invalid_payload' }, 400);
    }
  }

  let admin;
  let productSettingsMap;
  let variantProductMap;
  try {
    admin = createAdminClient(env);
    if (eventName === LEMONSQUEEZY_ORDER_CREATED_EVENT) {
      productSettingsMap = getProductSettingsMap(env);
      variantProductMap = getVariantProductMap(env, productSettingsMap);
    }
  } catch (error) {
    logWebhookError('config_error', {
      ...requestContext,
      eventName,
      orderId,
      message: error.message,
    });
    return json({ error: 'internal_error' }, 500);
  }

  try {
    if (eventName === LEMONSQUEEZY_ORDER_REFUNDED_EVENT) {
      logWebhook('refund_received', {
        ...requestContext,
        eventName,
        orderId,
      });

      if (!orderId) {
        logWebhookError('missing_order_id', {
          ...requestContext,
          eventName,
        });
        return json({ error: 'invalid_payload' }, 400);
      }

      const refundResult = await cancelLicencesByOrderId(admin, orderId);

      if (!refundResult.matchedCount) {
        logWebhook('refund_match_not_found', {
          ...requestContext,
          eventName,
          orderId,
          safeBackfillAvailable: refundResult.safeBackfillAvailable,
        });
        return json({ ok: true, ignored: true });
      }

      logWebhook('refund_match_found', {
        ...requestContext,
        eventName,
        orderId,
        matchedCount: refundResult.matchedCount,
        licenceIds: refundResult.matchedLicenceIds,
      });

      if (refundResult.cancelledLicenceIds.length) {
        logWebhook('licence_cancelled', {
          ...requestContext,
          eventName,
          orderId,
          licenceIds: refundResult.cancelledLicenceIds,
        });
      }

      if (refundResult.alreadyCancelledLicenceIds.length) {
        logWebhook('licence_already_cancelled', {
          ...requestContext,
          eventName,
          orderId,
          licenceIds: refundResult.alreadyCancelledLicenceIds,
        });
      }

      return json({ ok: true });
    }

    const orderItem = order.first_order_item || {};
    const variantId = normalizeIdentifier(orderItem.variant_id);
    const productId = normalizeIdentifier(orderItem.product_id);
    const variantMapping = variantId ? variantProductMap[variantId] : null;

    if (!variantId || !variantMapping) {
      logWebhook('ignored', {
        ...requestContext,
        eventName,
        orderId,
        variantId,
        reason: 'variant_unmapped',
      });
      return json({ ok: true, ignored: true });
    }

    const { userId, userSource } = await resolveWebhookUser(admin, email);
    await syncProfile(admin, userId, email, requestContext, logWebhook);

    const licenceResult = await activateLicence(admin, userId, {
      productCode: variantMapping.productCode,
      plan: variantMapping.plan,
      orderId,
      productId,
      variantId,
    });

    logWebhook('activated', {
      ...requestContext,
      eventName,
      orderId,
      email,
      userId,
      userSource,
      productCode: variantMapping.productCode,
      variantId,
      licenceAction: licenceResult.action,
      licenceId: licenceResult.licenceId,
      previousPlan: licenceResult.previousPlan,
      previousStatus: licenceResult.previousStatus,
    });

    return json({ ok: true });
  } catch (error) {
    logWebhookError('activation_failed', {
      ...requestContext,
      eventName,
      orderId,
      email,
      message: error.message,
    });
    return json({ error: 'internal_error' }, 500);
  }
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return json({ ok: true });
    }

    const url = new URL(request.url);
    const pathname = url.pathname.replace(/\/+$/, '') || '/';

    if (pathname === '/health' && request.method === 'GET') {
      return json({ ok: true });
    }

    if (pathname === '/validate' && request.method === 'POST') {
      return handleValidate(request, env);
    }

    if (pathname === '/v1/auth/sign-in' && request.method === 'POST') {
      return handleAuthSignIn(request, env);
    }

    if (pathname === '/v1/auth/restore' && request.method === 'POST') {
      return handleAuthRestore(request, env);
    }

    if (pathname === '/v1/entitlements/validate' && request.method === 'POST') {
      return handleEntitlementsValidate(request, env);
    }

    if (pathname === '/v1/devices/check' && request.method === 'POST') {
      return handleDevicesCheck(request, env);
    }

    if (pathname === '/v1/devices/register' && request.method === 'POST') {
      return handleDevicesRegister(request, env);
    }

    if (pathname === '/lemonsqueezy/webhook' && request.method === 'POST') {
      return handleLemonSqueezyWebhook(request, env);
    }

    return json({ error: 'not_found' }, 404);
  },
};
