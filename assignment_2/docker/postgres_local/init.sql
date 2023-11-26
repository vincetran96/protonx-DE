GRANT ALL PRIVILEGES ON DATABASE "crypto_trading" TO postgres;

CREATE TABLE "public"."user_position" (
    user_id integer NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    position FLOAT NOT NULL,
    last_updated DATE NOT NULL
);


-- Data 2023-10-28
-- User 1
INSERT INTO "public"."user_position" VALUES (1, 'BNB', 20,'2023-10-28');
INSERT INTO "public"."user_position" VALUES (1, 'USDT',200.0,'2023-10-28');

-- User 2
INSERT INTO "public"."user_position" VALUES (2, 'ETH',0.4,'2023-10-28');
INSERT INTO "public"."user_position" VALUES (2, 'BTC',0.016,'2023-10-28');
INSERT INTO "public"."user_position" VALUES (2, 'USDT',1000.0,'2023-10-28');


-- Data 2023-10-29
-- User 1
INSERT INTO "public"."user_position" VALUES (1, 'BNB', 10,'2023-10-29');
INSERT INTO "public"."user_position" VALUES (1, 'USDT',2457.0,'2023-10-29');
INSERT INTO "public"."user_position" VALUES (1, 'ETH',1.3,'2023-10-29');

-- User 2
INSERT INTO "public"."user_position" VALUES (2, 'ETH',1.3,'2023-10-29');
INSERT INTO "public"."user_position" VALUES (2, 'BNB', 4.631,'2023-10-29');
INSERT INTO "public"."user_position" VALUES (2, 'USDT',500.0,'2023-10-28');

