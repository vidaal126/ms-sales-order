import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Param,
  ParseUUIDPipe,
  Post,
  Put,
  Query,
  UseInterceptors,
} from "@nestjs/common";
import type { DeliveryWindow } from "@domain/value-objects/delivery-window";
import {
  ChangeSalesOrderStatusUseCase,
  ChangeSalesOrderTransportUseCase,
  CreateSalesOrderUseCase,
  GetSalesOrderUseCase,
  ListSalesOrdersUseCase,
  RescheduleDeliveryUseCase,
  ScheduleDeliveryUseCase,
} from "@application/use-cases/sales-order.use-cases";
import { CorrelationId } from "./decorators/correlation-id.decorator";
import {
  ChangeStatusDto,
  ChangeTransportDto,
  CreateSalesOrderDto,
  DeliveryWindowDto,
  ListSalesOrdersQueryDto,
  type PaginatedSalesOrdersResponseDto,
  type SalesOrderResponseDto,
} from "./dto/sales-order.dto";
import { IdempotencyInterceptor } from "./interceptors/idempotency.interceptor";
import { toPaginatedSalesOrdersResponse, toSalesOrderResponse } from "./mappers/sales-order-response.mapper";

@Controller("sales-orders")
export class SalesOrderController {
  constructor(
    private readonly createOrder: CreateSalesOrderUseCase,
    private readonly getOrder: GetSalesOrderUseCase,
    private readonly listOrders: ListSalesOrdersUseCase,
    private readonly changeStatus: ChangeSalesOrderStatusUseCase,
    private readonly scheduleDelivery: ScheduleDeliveryUseCase,
    private readonly rescheduleDelivery: RescheduleDeliveryUseCase,
    private readonly changeTransport: ChangeSalesOrderTransportUseCase,
  ) {}

  @Post()
  @UseInterceptors(IdempotencyInterceptor)
  async create(
    @Body() dto: CreateSalesOrderDto,
    @CorrelationId() correlationId: string,
  ): Promise<SalesOrderResponseDto> {
    const order = await this.createOrder.execute(
      {
        customerId: dto.customerId,
        transportTypeId: dto.transportTypeId,
        notes: dto.notes,
        items: dto.items.map((line) => ({ itemId: line.itemId, quantity: line.quantity })),
      },
      { correlationId },
    );
    return toSalesOrderResponse(order);
  }

  @Get()
  async findAll(@Query() query: ListSalesOrdersQueryDto): Promise<PaginatedSalesOrdersResponseDto> {
    const dateFrom = query.dateFrom === undefined ? undefined : new Date(query.dateFrom);
    const dateTo = query.dateTo === undefined ? undefined : new Date(query.dateTo);
    if (dateFrom && dateTo && dateTo.getTime() < dateFrom.getTime()) {
      throw new BadRequestException("dateTo deve ser maior ou igual a dateFrom");
    }
    const output = await this.listOrders.execute({
      status: query.status,
      customerId: query.customerId,
      transportTypeId: query.transportTypeId,
      itemId: query.itemId,
      dateFrom,
      dateTo,
      page: query.page,
      limit: query.limit,
    });
    return toPaginatedSalesOrdersResponse(output);
  }

  @Get(":id")
  async findById(@Param("id", ParseUUIDPipe) id: string): Promise<SalesOrderResponseDto> {
    return toSalesOrderResponse(await this.getOrder.execute(id));
  }

  @Put(":id/status")
  async updateStatus(
    @Param("id", ParseUUIDPipe) id: string,
    @Body() dto: ChangeStatusDto,
    @CorrelationId() correlationId: string,
  ): Promise<SalesOrderResponseDto> {
    return toSalesOrderResponse(await this.changeStatus.execute(id, dto.status, { correlationId }));
  }

  @Post(":id/schedule")
  async schedule(
    @Param("id", ParseUUIDPipe) id: string,
    @Body() dto: DeliveryWindowDto,
    @CorrelationId() correlationId: string,
  ): Promise<SalesOrderResponseDto> {
    return toSalesOrderResponse(await this.scheduleDelivery.execute(id, toWindow(dto), { correlationId }));
  }

  @Put(":id/schedule")
  async reschedule(
    @Param("id", ParseUUIDPipe) id: string,
    @Body() dto: DeliveryWindowDto,
    @CorrelationId() correlationId: string,
  ): Promise<SalesOrderResponseDto> {
    return toSalesOrderResponse(await this.rescheduleDelivery.execute(id, toWindow(dto), { correlationId }));
  }

  @Put(":id/transport")
  async updateTransport(
    @Param("id", ParseUUIDPipe) id: string,
    @Body() dto: ChangeTransportDto,
    @CorrelationId() correlationId: string,
  ): Promise<SalesOrderResponseDto> {
    return toSalesOrderResponse(await this.changeTransport.execute(id, dto.transportTypeId, { correlationId }));
  }
}

function toWindow(dto: DeliveryWindowDto): DeliveryWindow {
  return {
    deliveryDate: new Date(`${dto.deliveryDate}T00:00:00.000Z`),
    windowStart: new Date(dto.windowStart),
    windowEnd: new Date(dto.windowEnd),
  };
}
