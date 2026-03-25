alter table public.licences
  add column if not exists product_code text,
  add column if not exists lemonsqueezy_order_id text,
  add column if not exists lemonsqueezy_product_id text,
  add column if not exists lemonsqueezy_variant_id text;

alter table public.devices
  add column if not exists product_code text;

create index if not exists licences_user_product_created_at_idx
  on public.licences (user_id, product_code, created_at desc);

create index if not exists devices_user_product_idx
  on public.devices (user_id, product_code);

create index if not exists devices_user_product_hash_idx
  on public.devices (user_id, product_code, device_hash);

update public.licences
set product_code = 'droptext-pdf'
where product_code is null;

update public.devices
set product_code = 'droptext-pdf'
where product_code is null;
