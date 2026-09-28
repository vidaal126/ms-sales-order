-- CreateTable
CREATE TABLE "idempotency_keys" (
    "key" TEXT NOT NULL,
    "requestHash" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "responseStatus" INTEGER,
    "responseBody" JSONB,
    "lockedAt" TIMESTAMP(3) NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "idempotency_keys_pkey" PRIMARY KEY ("key")
);

-- CreateTable
CREATE TABLE "outbox_events" (
    "id" TEXT NOT NULL,
    "aggregateId" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "schemaVersion" INTEGER NOT NULL,
    "correlationId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "publishedAt" TIMESTAMP(3),

    CONSTRAINT "outbox_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "items_replica" (
    "itemId" TEXT NOT NULL,
    "sku" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "unitPrice" DECIMAL(10,2) NOT NULL,
    "sourceEventId" TEXT NOT NULL,
    "sourceOccurredAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "items_replica_pkey" PRIMARY KEY ("itemId")
);

-- CreateTable
CREATE TABLE "customers_replica" (
    "customerId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "authorizedTransportTypeIds" TEXT[],
    "sourceEventId" TEXT NOT NULL,
    "sourceOccurredAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "customers_replica_pkey" PRIMARY KEY ("customerId")
);

-- CreateTable
CREATE TABLE "transport_types_replica" (
    "transportTypeId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "active" BOOLEAN NOT NULL,
    "sourceEventId" TEXT NOT NULL,
    "sourceOccurredAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "transport_types_replica_pkey" PRIMARY KEY ("transportTypeId")
);

-- CreateTable
CREATE TABLE "processed_events" (
    "eventId" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("eventId")
);

-- CreateTable
CREATE TABLE "sales_orders" (
    "id" TEXT NOT NULL,
    "customerId" TEXT NOT NULL,
    "transportTypeId" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "notes" TEXT,
    "version" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "sales_orders_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "sales_order_items" (
    "id" TEXT NOT NULL,
    "salesOrderId" TEXT NOT NULL,
    "itemId" TEXT NOT NULL,
    "quantity" INTEGER NOT NULL,
    "unitPrice" DECIMAL(10,2) NOT NULL,

    CONSTRAINT "sales_order_items_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "scheduling" (
    "id" TEXT NOT NULL,
    "salesOrderId" TEXT NOT NULL,
    "deliveryDate" DATE NOT NULL,
    "windowStart" TIMESTAMPTZ(3) NOT NULL,
    "windowEnd" TIMESTAMPTZ(3) NOT NULL,
    "confirmedAt" TIMESTAMP(3) NOT NULL,
    "rescheduledAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "scheduling_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "idempotency_keys_expiresAt_idx" ON "idempotency_keys"("expiresAt");

-- CreateIndex
CREATE INDEX "outbox_events_publishedAt_createdAt_idx" ON "outbox_events"("publishedAt", "createdAt");

-- CreateIndex
CREATE INDEX "sales_orders_customerId_idx" ON "sales_orders"("customerId");

-- CreateIndex
CREATE INDEX "sales_orders_transportTypeId_idx" ON "sales_orders"("transportTypeId");

-- CreateIndex
CREATE INDEX "sales_orders_status_idx" ON "sales_orders"("status");

-- CreateIndex
CREATE INDEX "sales_orders_createdAt_id_idx" ON "sales_orders"("createdAt", "id");

-- CreateIndex
CREATE INDEX "sales_order_items_itemId_idx" ON "sales_order_items"("itemId");

-- CreateIndex
CREATE UNIQUE INDEX "sales_order_items_salesOrderId_itemId_key" ON "sales_order_items"("salesOrderId", "itemId");

-- CreateIndex
CREATE UNIQUE INDEX "scheduling_salesOrderId_key" ON "scheduling"("salesOrderId");

-- CreateIndex
CREATE INDEX "scheduling_deliveryDate_idx" ON "scheduling"("deliveryDate");

-- AddForeignKey
ALTER TABLE "sales_order_items" ADD CONSTRAINT "sales_order_items_salesOrderId_fkey" FOREIGN KEY ("salesOrderId") REFERENCES "sales_orders"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "scheduling" ADD CONSTRAINT "scheduling_salesOrderId_fkey" FOREIGN KEY ("salesOrderId") REFERENCES "sales_orders"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- CHECK constraints escritas a mao (o Prisma nao as modela), espelhando o
-- dominio. Arrays escalares ficam no CHECK (NOT NULL) para nao divergir do
-- modelo Prisma.
ALTER TABLE "sales_orders"
  ADD CONSTRAINT "sales_orders_status_valid" CHECK ("status" IN ('CRIADA', 'PLANEJADA', 'AGENDADA', 'EM_TRANSPORTE', 'ENTREGUE')),
  ADD CONSTRAINT "sales_orders_notes_length" CHECK ("notes" IS NULL OR char_length("notes") BETWEEN 1 AND 1000),
  ADD CONSTRAINT "sales_orders_version_non_negative" CHECK ("version" >= 0),
  ADD CONSTRAINT "sales_orders_updated_after_created" CHECK ("updatedAt" >= "createdAt");

ALTER TABLE "sales_order_items"
  ADD CONSTRAINT "sales_order_items_quantity_range" CHECK ("quantity" BETWEEN 1 AND 10000),
  ADD CONSTRAINT "sales_order_items_unit_price_positive" CHECK ("unitPrice" > 0);

ALTER TABLE "scheduling"
  ADD CONSTRAINT "scheduling_window_order" CHECK ("windowStart" < "windowEnd");

ALTER TABLE "items_replica"
  ADD CONSTRAINT "items_replica_unit_price_positive" CHECK ("unitPrice" > 0);

ALTER TABLE "customers_replica"
  ADD CONSTRAINT "customers_replica_transports_not_null" CHECK ("authorizedTransportTypeIds" IS NOT NULL);

ALTER TABLE "outbox_events"
  ADD CONSTRAINT "outbox_events_schema_version_positive" CHECK ("schemaVersion" > 0);

ALTER TABLE "idempotency_keys"
  ADD CONSTRAINT "idempotency_keys_key_length" CHECK (char_length("key") BETWEEN 1 AND 255),
  ADD CONSTRAINT "idempotency_keys_status_valid" CHECK ("status" IN ('in_progress', 'completed')),
  ADD CONSTRAINT "idempotency_keys_completed_has_response" CHECK (
    "status" <> 'completed' OR ("responseStatus" IS NOT NULL AND "responseBody" IS NOT NULL)
  ),
  ADD CONSTRAINT "idempotency_keys_expires_after_lock" CHECK ("expiresAt" > "lockedAt");
