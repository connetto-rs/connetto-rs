-- The seed rows the demo's server serves from the products catalogue. The
-- stacks and the demo's tests apply it after the schema and the policies.
-- It is data, not DDL the clients translate into the replica.
INSERT INTO products (id, name, price_cents) VALUES
  ('10000000-0000-4000-8000-000000000001', 'Espresso machine', 29900),
  ('10000000-0000-4000-8000-000000000002', 'Moka pot', 3400),
  ('10000000-0000-4000-8000-000000000003', 'Pour-over cone', 1900),
  ('10000000-0000-4000-8000-000000000004', 'Gooseneck kettle', 8900),
  ('10000000-0000-4000-8000-000000000005', 'French press', 2500),
  ('10000000-0000-4000-8000-000000000006', 'Burr grinder', 14900),
  ('10000000-0000-4000-8000-000000000007', 'Tamper', 1200),
  ('10000000-0000-4000-8000-000000000008', 'Milk pitcher', 4500),
  ('10000000-0000-4000-8000-000000000009', 'AeroPress', 3900),
  ('10000000-0000-4000-8000-00000000000a', 'Ceramic mug', 1800),
  ('10000000-0000-4000-8000-00000000000b', 'Drip filter pack', 900),
  ('10000000-0000-4000-8000-00000000000c', 'Portafilter basket', 2200);
