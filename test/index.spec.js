import { beforeEach, describe, expect, it, vi } from 'vitest';

const DEFAULT_ENV = {
  SUPABASE_URL: 'https://example.supabase.co',
  SUPABASE_SECRET_KEY: 'service-role-key',
  LEMONSQUEEZY_WEBHOOK_SECRET: 'webhook-secret',
  LEMONSQUEEZY_VARIANT_PRODUCT_MAP: JSON.stringify({
    '111': { product_code: 'droptext-pdf', plan: 'basic' },
    '14411104': { product_code: 'submissions-pdf', plan: 'basic' },
  }),
};

const { createClientMock, getState, setState } = vi.hoisted(() => {
  const defaultSession = () => ({
    access_token: 'access-token',
    refresh_token: 'refresh-token',
    expires_in: 3600,
    expires_at: 2000000000,
    token_type: 'bearer',
  });

  const buildState = () => ({
    user: {
      id: 'user-123',
      email: 'user@example.com',
    },
    userError: null,
    authUsers: [],
    listUsersError: null,
    createdAuthUser: {
      id: 'created-user-123',
      email: 'created@example.com',
    },
    createUserError: null,
    signInData: {
      user: {
        id: 'user-123',
        email: 'user@example.com',
      },
      session: defaultSession(),
    },
    signInError: null,
    refreshData: {
      session: {
        ...defaultSession(),
        access_token: 'restored-access-token',
        refresh_token: 'restored-refresh-token',
      },
    },
    refreshError: null,
    licences: [
      {
        id: 'lic-droptext-1',
        user_id: 'user-123',
        product_code: null,
        plan: 'pro',
        status: 'active',
        expires_at: '2099-01-01T00:00:00.000Z',
        created_at: '2026-01-01T00:00:00.000Z',
      },
    ],
    licenceError: null,
    licenceUpdateError: null,
    licenceInsertError: null,
    devices: [],
    devicesError: null,
    profiles: [],
    profileSelectError: null,
    profileUpsertError: null,
    deviceInsertError: null,
    deviceUpdateError: null,
    profileUpserts: [],
    deviceInserts: [],
    deviceUpdates: [],
    licenceInserts: [],
    licenceUpdates: [],
    createUserCalls: [],
    listUsersCalls: [],
  });

  const selectBuilder = (table, state) => ({
    eq: vi.fn(async (_column, value) => {
      if (table === 'licences') {
        return {
          data: state.licences.filter((licence) => licence.user_id === value),
          error: state.licenceError,
        };
      }

      if (table === 'devices') {
        return {
          data: state.devices.filter((device) => device.user_id === value),
          error: state.devicesError,
        };
      }

      throw new Error(`Unexpected eq table: ${table}`);
    }),
  });

  const createTableBuilder = (table, state) => {
    if (table === 'profiles') {
      return {
        upsert: vi.fn(async (payload) => {
          state.profileUpserts.push(payload);
          return { data: null, error: state.profileUpsertError };
        }),
        select: vi.fn(() => ({
          ilike: vi.fn((_column, email) => ({
            limit: vi.fn(() => ({
              maybeSingle: vi.fn(async () => ({
                data:
                  state.profiles.find(
                    (profile) => profile.email.toLowerCase() === email.toLowerCase(),
                  ) || null,
                error: state.profileSelectError,
              })),
            })),
          })),
        })),
      };
    }

    if (table === 'licences') {
      return {
        select: vi.fn(() => selectBuilder('licences', state)),
        update: vi.fn((payload) => ({
          eq: vi.fn(async (_column, id) => {
            state.licenceUpdates.push({ id, payload });
            return { data: null, error: state.licenceUpdateError };
          }),
        })),
        insert: vi.fn(async (payload) => {
          state.licenceInserts.push(payload);
          return { data: null, error: state.licenceInsertError };
        }),
      };
    }

    if (table === 'devices') {
      return {
        select: vi.fn(() => selectBuilder('devices', state)),
        insert: vi.fn(async (payload) => {
          state.deviceInserts.push(payload);
          return { data: null, error: state.deviceInsertError };
        }),
        update: vi.fn((payload) => ({
          eq: vi.fn(async (_column, id) => {
            state.deviceUpdates.push({ id, payload });
            return { data: null, error: state.deviceUpdateError };
          }),
        })),
      };
    }

    throw new Error(`Unexpected table: ${table}`);
  };

  let state = buildState();

  return {
    createClientMock: vi.fn(() => ({
      auth: {
        getUser: vi.fn(async () => ({
          data: state.user ? { user: state.user } : { user: null },
          error: state.userError,
        })),
        signInWithPassword: vi.fn(async () => ({
          data: state.signInData,
          error: state.signInError,
        })),
        refreshSession: vi.fn(async () => ({
          data: state.refreshData,
          error: state.refreshError,
        })),
        admin: {
          listUsers: vi.fn(async ({ page, perPage }) => {
            state.listUsersCalls.push({ page, perPage });
            const start = (page - 1) * perPage;
            const users = state.authUsers.slice(start, start + perPage);
            return {
              data: { users },
              error: state.listUsersError,
            };
          }),
          createUser: vi.fn(async (payload) => {
            state.createUserCalls.push(payload);
            if (state.createUserError) {
              return {
                data: { user: null },
                error: state.createUserError,
              };
            }

            const createdUser = {
              ...state.createdAuthUser,
              email: payload.email,
            };
            state.authUsers.push(createdUser);
            return {
              data: { user: createdUser },
              error: null,
            };
          }),
        },
      },
      from: vi.fn((table) => createTableBuilder(table, state)),
    })),
    getState: () => state,
    setState: (overrides = {}) => {
      state = { ...buildState(), ...overrides };
    },
  };
});

vi.mock('@supabase/supabase-js', () => ({
  createClient: createClientMock,
}));

import worker from '../src/index.js';

async function signPayload(secret, payload) {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(payload),
  );

  return Array.from(new Uint8Array(signature), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

describe('licensing worker', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setState();
  });

  it('serves /health without creating a Supabase client', async () => {
    const response = await worker.fetch(new Request('http://example.com/health'), {});

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
    expect(createClientMock).not.toHaveBeenCalled();
  });

  it('keeps legacy /validate behaviour and defaults missing product_code to droptext-pdf', async () => {
    setState({
      devices: [{ id: 'dev-1', user_id: 'user-123', product_code: null, device_hash: 'device-1' }],
    });

    const response = await worker.fetch(
      new Request('http://example.com/validate', {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: JSON.stringify({ deviceHash: 'device-1', deviceName: 'Office Laptop' }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      valid: true,
      plan: 'pro',
      status: 'active',
      user: {
        id: 'user-123',
        email: 'user@example.com',
      },
      offlineGraceDays: 7,
    });
    expect(getState().deviceUpdates).toHaveLength(1);
    expect(getState().deviceUpdates[0].payload.product_code).toBe('droptext-pdf');
  });

  it('prioritises an older active licence over a newer inactive licence for the same product', async () => {
    setState({
      licences: [
        {
          id: 'lic-old-active',
          user_id: 'user-123',
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: '2099-01-01T00:00:00.000Z',
          created_at: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'lic-new-paused',
          user_id: 'user-123',
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'paused',
          expires_at: '2099-01-01T00:00:00.000Z',
          created_at: '2026-02-01T00:00:00.000Z',
        },
      ],
    });

    const response = await worker.fetch(
      new Request('http://example.com/v1/entitlements/validate', {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: JSON.stringify({ product_code: 'submissions-pdf' }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect((await response.json()).entitlement).toEqual({
      product_code: 'submissions-pdf',
      plan: 'basic',
      status: 'active',
      expires_at: '2099-01-01T00:00:00.000Z',
    });
  });

  it('rejects validation for the wrong product entitlement', async () => {
    setState({
      licences: [
        {
          id: 'lic-submissions',
          user_id: 'user-123',
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: '2099-01-01T00:00:00.000Z',
          created_at: '2026-01-01T00:00:00.000Z',
        },
      ],
    });

    const response = await worker.fetch(
      new Request('http://example.com/v1/entitlements/validate', {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: JSON.stringify({ product_code: 'droptext-pdf' }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(403);
    expect(await response.json()).toEqual({
      ok: false,
      reason: 'no_entitlement',
    });
  });

  it('returns valid false on /validate when a droptext-only user requests submissions-pdf', async () => {
    setState({
      licences: [
        {
          id: 'lic-droptext-only',
          user_id: 'user-123',
          product_code: 'droptext-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: '2099-01-01T00:00:00.000Z',
          created_at: '2026-01-01T00:00:00.000Z',
        },
      ],
    });

    const response = await worker.fetch(
      new Request('http://example.com/validate', {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: JSON.stringify({
          product_code: 'submissions-pdf',
          deviceHash: 'device-1',
          deviceName: 'Office Laptop',
        }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(403);
    expect(await response.json()).toEqual({
      valid: false,
      reason: 'no_licence',
    });
  });

  it('enforces device limits per product instead of globally', async () => {
    setState({
      licences: [
        {
          id: 'lic-droptext',
          user_id: 'user-123',
          product_code: 'droptext-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: null,
          created_at: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'lic-submissions',
          user_id: 'user-123',
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: null,
          created_at: '2026-01-02T00:00:00.000Z',
        },
      ],
      devices: [
        { id: 'dev-1', user_id: 'user-123', product_code: 'droptext-pdf', device_hash: 'drop-1' },
        { id: 'dev-2', user_id: 'user-123', product_code: 'droptext-pdf', device_hash: 'drop-2' },
        { id: 'dev-3', user_id: 'user-123', product_code: 'submissions-pdf', device_hash: 'sub-1' },
      ],
    });

    const response = await worker.fetch(
      new Request('http://example.com/v1/devices/register', {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: JSON.stringify({
          product_code: 'submissions-pdf',
          device_hash: 'sub-2',
          device_name: 'Second submissions device',
        }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      ok: true,
      product_code: 'submissions-pdf',
      device: {
        status: 'registered',
        count: 2,
        limit: 2,
      },
    });
    expect(getState().deviceInserts).toHaveLength(1);
    expect(getState().deviceInserts[0].product_code).toBe('submissions-pdf');
  });

  it('returns multiple entitlements on sign-in', async () => {
    setState({
      licences: [
        {
          id: 'lic-droptext',
          user_id: 'user-123',
          product_code: null,
          plan: 'pro',
          status: 'active',
          expires_at: null,
          created_at: '2026-01-01T00:00:00.000Z',
        },
        {
          id: 'lic-submissions',
          user_id: 'user-123',
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: null,
          created_at: '2026-01-02T00:00:00.000Z',
        },
      ],
    });

    const response = await worker.fetch(
      new Request('http://example.com/v1/auth/sign-in', {
        method: 'POST',
        body: JSON.stringify({
          email: 'user@example.com',
          password: 'secret-password',
        }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      ok: true,
      session: {
        access_token: 'access-token',
        refresh_token: 'refresh-token',
        expires_in: 3600,
        expires_at: 2000000000,
        token_type: 'bearer',
      },
      account: {
        id: 'user-123',
        email: 'user@example.com',
      },
      entitlements: [
        {
          product_code: 'droptext-pdf',
          plan: 'pro',
          status: 'active',
          expires_at: null,
        },
        {
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: null,
        },
      ],
    });
  });

  it('checks device status for a specific product', async () => {
    setState({
      licences: [
        {
          id: 'lic-submissions',
          user_id: 'user-123',
          product_code: 'submissions-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: null,
          created_at: '2026-01-02T00:00:00.000Z',
        },
      ],
      devices: [
        { id: 'dev-1', user_id: 'user-123', product_code: 'submissions-pdf', device_hash: 'sub-1' },
      ],
    });

    const response = await worker.fetch(
      new Request('http://example.com/v1/devices/check', {
        method: 'POST',
        headers: { authorization: 'Bearer valid-token' },
        body: JSON.stringify({
          product_code: 'submissions-pdf',
          device_hash: 'sub-1',
        }),
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      ok: true,
      product_code: 'submissions-pdf',
      device: {
        registered: true,
        count: 1,
        limit: 2,
        can_register: true,
      },
    });
  });

  it('ignores a Lemon Squeezy webhook when the variant_id is unmapped', async () => {
    const payload = JSON.stringify({
      meta: { event_name: 'order_created' },
      data: {
        id: 'order-1',
        attributes: {
          status: 'paid',
          user_email: 'buyer@example.com',
          first_order_item: {
            product_id: 1,
            variant_id: 999,
          },
        },
      },
    });
    const signature = await signPayload('webhook-secret', payload);

    const response = await worker.fetch(
      new Request('http://example.com/lemonsqueezy/webhook', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-event-name': 'order_created',
          'x-signature': signature,
        },
        body: payload,
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true, ignored: true });
    expect(getState().licenceUpdates).toHaveLength(0);
    expect(getState().licenceInserts).toHaveLength(0);
  });

  it('activates the mapped product for a valid Lemon Squeezy webhook', async () => {
    setState({
      profiles: [{ id: 'user-123', email: 'buyer@example.com' }],
      licences: [
        {
          id: 'lic-legacy-droptext',
          user_id: 'user-123',
          product_code: null,
          plan: 'trial',
          status: 'trial',
          expires_at: '2099-01-01T00:00:00.000Z',
          created_at: '2026-01-01T00:00:00.000Z',
        },
      ],
    });

    const payload = JSON.stringify({
      meta: { event_name: 'order_created' },
      data: {
        id: 'order-2',
        attributes: {
          status: 'paid',
          user_email: 'buyer@example.com',
          first_order_item: {
            product_id: 1,
            variant_id: 111,
          },
        },
      },
    });
    const signature = await signPayload('webhook-secret', payload);

    const response = await worker.fetch(
      new Request('http://example.com/lemonsqueezy/webhook', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-event-name': 'order_created',
          'x-signature': signature,
        },
        body: payload,
      }),
      DEFAULT_ENV,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
    expect(getState().licenceUpdates).toEqual([
      {
        id: 'lic-legacy-droptext',
        payload: {
          user_id: 'user-123',
          product_code: 'droptext-pdf',
          plan: 'basic',
          status: 'active',
          expires_at: null,
          lemonsqueezy_order_id: 'order-2',
          lemonsqueezy_product_id: '1',
          lemonsqueezy_variant_id: '111',
        },
      },
    ]);
  });
});
