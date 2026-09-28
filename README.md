# ms-sales-order

Microsserviço de ordem de venda (NestJS, Prisma, PostgreSQL, Kafka). É o núcleo
extraído do monolito `sales-order-api`: pedido, linhas e agendamento de
entrega.

- **HTTP**: cria e consulta pedidos e faz as transições de status,
  agendamento e troca de transporte.
- **Kafka (entrada)**: mantém três réplicas locais, só com o que o pedido
  precisa. Não há chamada síncrona a outros serviços no caminho crítico.
- **Kafka (saída)**: publica os eventos `sales-order.*` via Transactional
  Outbox.

## Arquitetura

Camadas hexagonais, com dependências apontando para dentro:

- `src/domain`:
  - agregado `SalesOrder` (linhas e agendamento dentro do agregado);
  - value objects de status (máquina de estados), dinheiro em centavos e janela
    de entrega;
  - eventos e erros de domínio.

  Não depende de Nest, Prisma nem Kafka.
- `src/application`: use cases de pedido (`CreateSalesOrder`,
  `ChangeSalesOrderStatus`, `ScheduleDelivery`, `RescheduleDelivery`,
  `ChangeSalesOrderTransport`, `Get`/`List`) e de réplica (`Sync*Replica`).
  Os ports ficam em `application/ports`: `ISalesOrderRepository`,
  `IReplicaRepository`/`IReplicaReader`, `SyncReplicaPort` e `DeadLetterPort`.
- `src/infrastructure`:
  - Prisma (pedido com controle otimista por `version`, réplicas com upsert
    condicional);
  - Kafka: decoders Zod, consumers, DLT, outbox publisher;
  - HTTP com Idempotency-Key, health e métricas.

  A composição dos use cases fica em `SalesOrderModule` (`useFactory`). Nenhum
  código é compartilhado com outros serviços: o contrato é o formato do evento.

### Máquina de estados

```
CRIADA -> PLANEJADA -> AGENDADA -> EM_TRANSPORTE -> ENTREGUE
```

O status só avança um passo por vez: sem pulo e sem volta, e `ENTREGUE` é
final.

| Transição | Como | Regras |
|---|---|---|
| CRIADA para PLANEJADA | `PUT /sales-orders/:id/status` | só a máquina de estados |
| PLANEJADA para AGENDADA | **só** `POST /sales-orders/:id/schedule` | não pode haver agendamento prévio; janela válida; agendamento, status e outbox na mesma transação |
| AGENDADA para EM_TRANSPORTE | `PUT /status` | exige agendamento |
| EM_TRANSPORTE para ENTREGUE | `PUT /status` | só a máquina de estados |

Outras operações:

- **Reagendar** (`PUT /schedule`): exige AGENDADA. Atualiza as datas e
  `rescheduledAt`, mantém `confirmedAt` e o status, e pode ser repetido.
- **Trocar transporte** (`PUT /transport`): bloqueado em EM_TRANSPORTE e
  ENTREGUE. O novo transporte precisa estar ativo e autorizado para o cliente.
  Informar o mesmo transporte é no-op, sem evento.

Janela de entrega (`validateWindow`):

1. datas válidas;
2. `windowStart < windowEnd`;
3. `windowStart >= agora`;
4. `deliveryDate` até 365 dias à frente;
5. janela no mesmo dia UTC da `deliveryDate`.

### Criação do pedido

Um pedido só é criado se o cliente, o transporte e os itens passam nestas
checagens contra as réplicas locais:

- **Cliente**: existe na `customers_replica`.
- **Transporte**:
  - existe na `transport_types_replica`;
  - está ativo;
  - consta em `authorizedTransportTypeIds` do cliente.
- **Itens**: todos existem na `items_replica`. Se faltar algum, o 422 lista
  todos os ids faltantes.
- **Linhas**:
  - pelo menos 1 e no máximo 100;
  - nenhum item repetido;
  - `quantity` inteira entre 1 e 10000.

**Preço congelado.** O `unitPrice` é copiado da réplica na criação e não muda
depois, mesmo que a réplica mude.

**Total.** É a soma de `quantity * unitPrice` calculada em centavos inteiros.
Isso evita erro de ponto flutuante: `0.1 * 3 + 0.2` dá `0.5`, e não
`0.5000000000000001`.

**Consistência eventual.** Um cliente, item ou transporte recém-cadastrado pode
ainda não ter chegado à réplica. Nesse caso a criação responde 422 até o evento
ser consumido.

### Réplicas

| Réplica | Tópicos | Consumer group | Campos |
|---|---|---|---|
| `items_replica` | `catalog.ItemCreated` | `ms-sales-order.catalog-item-sync` | `itemId`, `sku`, `name`, `unitPrice` DECIMAL(10,2) |
| `customers_replica` | `customer.CustomerCreated`, `customer.CustomerUpdated` | `ms-sales-order.customer-sync` | `customerId`, `name`, `authorizedTransportTypeIds` |
| `transport_types_replica` | `transport.TransportTypeCreated`, `transport.TransportTypeUpdated` | `ms-sales-order.transport-type-sync` | `transportTypeId`, `name`, `active` |

Todas as réplicas seguem estas regras:

- **Consumo**: `fromBeginning: true` e `autoCommit: false`. O commit de
  `offset + 1` só acontece depois da transação Prisma, do ack da DLT ou da
  detecção de duplicata.
- **Idempotência**: o `eventId` entra em `processed_events` na MESMA transação
  do upsert. Um evento já processado é ignorado (`duplicate`).
- **Reordenação**: o upsert só sobrescreve se o `occurredAt` do evento for mais
  recente que o `sourceOccurredAt` gravado; caso contrário, o evento vira
  `stale`. `Created` e `Updated` chegam por tópicos diferentes, sem ordem
  garantida entre si, e é esse guard que resolve. Um `Updated` que chega antes
  do `Created` mais antigo prevalece.
- **Formato**: envelope v2 (`schemaVersion: 2` no envelope), com `payload.id`
  igual ao `aggregateId`.
- **Legado de `catalog.ItemCreated`**: também aceita o formato v1, com
  `schemaVersion: 1` dentro do payload e sem `eventId`. O `eventId` é derivado
  por UUID v5 de `topico:particao:offset`, com um namespace fixo próprio deste
  serviço. O legado sem `schemaVersion` vai para a DLT.

### Erros

| Tipo | Exemplos | Tratamento |
|---|---|---|
| Não recuperável | JSON inválido, schema Zod, `schemaVersion` não suportada, `unitPrice` inválido (<= 0 ou com mais de 2 casas), dado rejeitado pelo banco | vai para a DLT do serviço com os bytes originais; o offset é commitado só depois do ack |
| Recuperável | banco indisponível, timeout, erro não classificado | sem commit e sem DLT; retry em processo com backoff exponencial e jitter (`CONSUMER_RETRY_*`); esgotado, pausa a partição por `CONSUMER_PAUSE_MS` e retoma da mesma mensagem |

Mapeamento HTTP:

| Situação | Status |
|---|---|
| pedido inexistente | 404 |
| alteração concorrente (versão mudou entre leitura e gravação) | 409 |
| Idempotency-Key em processamento | 409 |
| regras de domínio (transição, janela, cliente/itens/transporte desconhecidos ou não autorizados) | 422 |
| Idempotency-Key com corpo diferente | 422 |
| validação do corpo | 400 |

**DLT por serviço consumidor.** O ms-transport e o ms-customer também consomem
`catalog.ItemCreated` e `transport.*`, então a DLT deste serviço tem nome
próprio: `<topico>.ms-sales-order.DLT`, por exemplo
`catalog.ItemCreated.ms-sales-order.DLT`.

Headers da DLT:

- `dlt-reason`, `dlt-detail`;
- `dlt-source-topic`, `dlt-source-partition`, `dlt-source-offset`,
  `dlt-source-timestamp`;
- `dlt-failed-at`, `dlt-consumer-group`;
- os headers originais.

## Eventos publicados

Todos os eventos saem via outbox, na mesma transação da mudança, com:

- envelope v2 `{eventId, eventType, schemaVersion: 2, occurredAt, aggregateId, correlationId, payload}`;
- key = id do pedido;
- headers `eventType`, `schemaVersion` e `correlationId`.

| Tópico | Payload |
|---|---|
| `sales-order.OrderCreated` | `{id, customerId, transportTypeId, status, items: [{itemId, quantity, unitPrice}], total}` |
| `sales-order.OrderStatusChanged` | `{id, previousStatus, currentStatus}` |
| `sales-order.DeliveryScheduled` | `{id, deliveryDate, windowStart, windowEnd}` |
| `sales-order.DeliveryRescheduled` | `{id, previousDeliveryDate, deliveryDate, windowStart, windowEnd}` |
| `sales-order.TransportChanged` | `{id, previousTransportTypeId, transportTypeId}` |

Agendar publica `DeliveryScheduled` **e** `OrderStatusChanged` (de PLANEJADA
para AGENDADA).

## Como subir

Com a infraestrutura do [ms-platform](../ms-platform/README.md) no ar:

```bash
cp .env.example .env
yarn install
yarn prisma migrate deploy
yarn start
```

O projeto fixa Yarn 1 (`packageManager: yarn@1.22.22`). No compose, o job
`migrate` (target `migrate` do Dockerfile) aplica as migrations antes do app
subir.

## Variáveis de ambiente

Validadas com Zod no boot: env inválida impede a subida, com a lista completa de
problemas. Veja `.env.example`.

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | obrigatória | database `sales_order` |
| `KAFKA_BROKER` | obrigatória | `host:porta`, separados por vírgula |
| `KAFKA_SEND_TIMEOUT_MS` | 5000 | teto de cada envio ao Kafka (outbox e DLT); estourado, conta como falha de envio |
| `CATALOG_ITEM_SYNC_GROUP_ID` / `CUSTOMER_SYNC_GROUP_ID` / `TRANSPORT_TYPE_SYNC_GROUP_ID` | `ms-sales-order.*-sync` | groups das réplicas |
| `CONSUMER_RETRY_RETRIES` / `_INITIAL_MS` / `_MAX_MS` | 5 / 300 / 30000 | retry de erro recuperável |
| `CONSUMER_PAUSE_MS` | 30000 | pausa da partição depois de esgotar o retry |
| `OUTBOX_POLL_INTERVAL_MS` / `OUTBOX_BATCH_SIZE` | 2000 / 20 | publisher do outbox |
| `IDEMPOTENCY_TTL_HOURS` / `_LOCK_TIMEOUT_MS` / `_CLEANUP_INTERVAL_MS` | 24 / 30000 / 3600000 | Idempotency-Key |
| `THROTTLE_DEFAULT_TTL_MS` / `_LIMIT` | 60000 / 100 | rate limit por IP |
| `PORT` | 3004 | porta HTTP |

## API HTTP

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/sales-orders` | `{customerId, transportTypeId, notes?, items: [{itemId, quantity}]}` retorna 201; `Idempotency-Key` opcional |
| `GET` | `/sales-orders` | filtros `status`, `customerId`, `transportTypeId`, `itemId`, `dateFrom`/`dateTo` (em `createdAt`; `dateTo < dateFrom` dá 400), `page`, `limit` (até 100); resposta com `total`; ordem `createdAt desc, id desc` |
| `GET` | `/sales-orders/:id` | pedido com `items` (e `lineTotal`), `total` e `scheduling` |
| `PUT` | `/sales-orders/:id/status` | `{status}`: PLANEJADA, EM_TRANSPORTE ou ENTREGUE; AGENDADA dá 422 |
| `POST` | `/sales-orders/:id/schedule` | `{deliveryDate: "YYYY-MM-DD", windowStart, windowEnd}` (ISO 8601) |
| `PUT` | `/sales-orders/:id/schedule` | reagendamento, com o mesmo corpo |
| `PUT` | `/sales-orders/:id/transport` | `{transportTypeId}` |
| `GET` | `/health/live` / `/health/ready` | readiness: banco, broker e os três consumers |
| `GET` | `/metrics` | Prometheus: HTTP por template de rota, consumo por outcome, outbox |

Com **Idempotency-Key**:

- mesma chave e mesmo corpo: devolve a resposta original, com
  `Idempotent-Replayed: true`;
- corpo diferente: 422;
- requisição original ainda em andamento: 409.

## Tópicos, groups, replay e DLT

Os tópicos são criados pelo `kafka-init` do ms-platform (auto-create
desligado). O serviço também garante as DLTs pelo admin do KafkaJS
(`ensureTopic`).

**Replay** de uma réplica:

- **Opção 1: resetar o group.** Com o serviço parado:

  ```bash
  docker compose -f ../ms-platform/docker-compose.yml exec kafka kafka-consumer-groups \
    --bootstrap-server kafka:29092 --group ms-sales-order.catalog-item-sync \
    --topic catalog.ItemCreated --reset-offsets --to-earliest --execute
  ```

- **Opção 2: group temporário.** Suba uma instância com o group fixo por
  execução, por exemplo
  `CATALOG_ITEM_SYNC_GROUP_ID=ms-sales-order.catalog-item-sync.replay-20261001`.

A idempotência (`processed_events`) e o guard de `sourceOccurredAt` tornam o
replay seguro.

**Inspecionar a DLT:**

```bash
docker compose -f ../ms-platform/docker-compose.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic catalog.ItemCreated.ms-sales-order.DLT \
  --from-beginning --property print.headers=true
```

## Testes

```bash
yarn lint && yarn typecheck
yarn test              # unitários
yarn test:integration  # Postgres e Kafka reais (testcontainers)
```

**Unitários**:

- máquina de estados, com todas as transições;
- `validateWindow`, uma regra por caso;
- total em centavos e linhas;
- troca de transporte e eventos;
- use cases com fakes, incluindo a concorrência otimista;
- decoders dos três consumers, incluindo o v1 legado;
- consumer de réplica (DLT com group, invariante, erro recuperável).

**Integração**: publica o histórico de itens, clientes e transportes antes de
o app subir e verifica:

- réplicas construídas e o guard de ordem;
- poison message e preço zero na DLT do serviço;
- criação com preço congelado e envelope em `sales-order.OrderCreated`;
- os 422 de cliente, itens e transporte;
- o ciclo completo de status com agendamento, reagendamento e troca de
  transporte;
- Idempotency-Key;
- filtros de listagem;
- `/metrics` e readiness.

## Lacunas do monolito corrigidas

| Monolito | Aqui |
|---|---|
| `PUT /status` levava a AGENDADA sem agendamento (e o pedido nunca mais podia ser agendado) | AGENDADA só via `POST /schedule`; EM_TRANSPORTE exige agendamento |
| `quantity > 0` só no DTO | validada no domínio (inteira, 1 a 10000) e em CHECK no banco |
| sem total | `total` calculado em centavos e retornado, e `lineTotal` por linha |
| cliente ou transporte inexistente dava 422 genérico | cliente e itens desconhecidos com erro próprio (422, listando os ids); pedido inexistente dá 404 |
| Idempotency-Key global, sem hash do corpo | chave com hash de método, URL e corpo; corpo diferente dá 422 |
| agendar não emitia mudança de status | `DeliveryScheduled` e `OrderStatusChanged` |
| sem CHECK no banco | CHECK de status, quantidade, preço, janela e notas |
| sem controle de concorrência | versão otimista: gravação concorrente dá 409 |

## Limitações conhecidas

- **Consistência eventual das réplicas.** Um cadastro recente em outro serviço
  pode dar 422 por alguns instantes.
- **Sem atualização de preço.** O catálogo não publica `ItemUpdated`: preço e
  nome não se atualizam na réplica (o pedido congela o preço de qualquer
  forma).
- **eventId de v1 derivado da posição.** O mesmo evento republicado em outro
  offset ganha outro id; o guard de `sourceOccurredAt` impede que ele altere a
  réplica.
- **Replay republica na DLT** as mensagens não recuperáveis.
- **Erro não classificado é tratado como recuperável.** Nunca perde evento, mas
  um erro permanente mantém a partição em ciclos de pausa e retomada, com o
  readiness `down`.
- **Várias réplicas do serviço.** O outbox não usa `SKIP LOCKED` (publicação
  duplicada, segura para consumidores idempotentes), e o rate limit fica em
  memória.
- **Sem autenticação no serviço.** Identidade e autorização vêm do
  [ms-gateway](../ms-gateway/README.md) (`x-user-*`), e o serviço confia na
  rede interna.
