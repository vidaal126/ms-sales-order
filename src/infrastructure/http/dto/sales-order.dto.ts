import { Type } from "class-transformer";
import {
  ArrayMaxSize,
  ArrayMinSize,
  IsArray,
  IsIn,
  IsInt,
  IsISO8601,
  IsOptional,
  IsString,
  IsUUID,
  Matches,
  Max,
  MaxLength,
  Min,
  ValidateNested,
} from "class-validator";
import { MAX_LINES, MAX_QUANTITY, MIN_QUANTITY, NOTES_MAX_LENGTH } from "@domain/entities/sales-order.entity";
import { ORDER_STATUSES } from "@domain/value-objects/order-status";

export class OrderLineDto {
  @IsUUID()
  readonly itemId!: string;

  @IsInt()
  @Min(MIN_QUANTITY)
  @Max(MAX_QUANTITY)
  readonly quantity!: number;
}

export class CreateSalesOrderDto {
  @IsUUID()
  readonly customerId!: string;

  @IsUUID()
  readonly transportTypeId!: string;

  @IsOptional()
  @IsString()
  @MaxLength(NOTES_MAX_LENGTH)
  readonly notes?: string;

  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(MAX_LINES)
  @ValidateNested({ each: true })
  @Type(() => OrderLineDto)
  readonly items!: OrderLineDto[];
}

// Aceita qualquer status valido: o dominio responde 422 com o motivo (ex.:
// AGENDADA so via agendamento), mais claro que um 400 generico.
export class ChangeStatusDto {
  @IsIn(ORDER_STATUSES)
  readonly status!: (typeof ORDER_STATUSES)[number];
}

export class DeliveryWindowDto {
  // Data de entrega (sem hora), ex.: 2026-10-15.
  @Matches(/^\d{4}-\d{2}-\d{2}$/, { message: "deliveryDate deve estar no formato YYYY-MM-DD" })
  readonly deliveryDate!: string;

  @IsISO8601({ strict: true, strictSeparator: true })
  readonly windowStart!: string;

  @IsISO8601({ strict: true, strictSeparator: true })
  readonly windowEnd!: string;
}

export class ChangeTransportDto {
  @IsUUID()
  readonly transportTypeId!: string;
}

export class ListSalesOrdersQueryDto {
  @IsOptional()
  @IsIn(ORDER_STATUSES)
  readonly status?: (typeof ORDER_STATUSES)[number];

  @IsOptional()
  @IsUUID()
  readonly customerId?: string;

  @IsOptional()
  @IsUUID()
  readonly transportTypeId?: string;

  @IsOptional()
  @IsUUID()
  readonly itemId?: string;

  @IsOptional()
  @IsISO8601({ strict: true })
  readonly dateFrom?: string;

  @IsOptional()
  @IsISO8601({ strict: true })
  readonly dateTo?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  readonly page?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  readonly limit?: number;
}

export class OrderLineResponseDto {
  readonly itemId!: string;
  readonly quantity!: number;
  readonly unitPrice!: number;
  readonly lineTotal!: number;
}

export class SchedulingResponseDto {
  readonly deliveryDate!: string;
  readonly windowStart!: string;
  readonly windowEnd!: string;
  readonly confirmedAt!: string;
  readonly rescheduledAt!: string | null;
}

export class SalesOrderResponseDto {
  readonly id!: string;
  readonly customerId!: string;
  readonly transportTypeId!: string;
  readonly status!: string;
  readonly notes!: string | null;
  readonly items!: OrderLineResponseDto[];
  readonly total!: number;
  readonly scheduling!: SchedulingResponseDto | null;
  readonly createdAt!: string;
  readonly updatedAt!: string;
}

export class PaginatedSalesOrdersResponseDto {
  readonly items!: SalesOrderResponseDto[];
  readonly total!: number;
  readonly page!: number;
  readonly pageSize!: number;
}
