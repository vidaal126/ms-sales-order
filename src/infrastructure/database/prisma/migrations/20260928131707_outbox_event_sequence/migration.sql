/*
  Warnings:

  - A unique constraint covering the columns `[sequence]` on the table `outbox_events` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "outbox_events_publishedAt_createdAt_idx";

-- AlterTable
ALTER TABLE "outbox_events" ADD COLUMN     "sequence" BIGSERIAL NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "outbox_events_sequence_key" ON "outbox_events"("sequence");

-- CreateIndex
CREATE INDEX "outbox_events_publishedAt_sequence_idx" ON "outbox_events"("publishedAt", "sequence");
