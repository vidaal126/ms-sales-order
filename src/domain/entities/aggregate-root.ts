import type { DomainEvent } from "@domain/events/domain-event";

export abstract class AggregateRoot<TEvent extends DomainEvent = DomainEvent> {
  private domainEvents: TEvent[] = [];

  protected record(event: TEvent): void {
    this.domainEvents.push(event);
  }

  // Devolve e limpa os eventos pendentes: quem persiste o agregado e o unico
  // consumidor, e um segundo save nao pode republicar o mesmo fato.
  pullDomainEvents(): readonly TEvent[] {
    const events = this.domainEvents;
    this.domainEvents = [];
    return events;
  }
}
