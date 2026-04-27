package com.example.kafkametrics.control;

import jakarta.persistence.*;

import java.time.Instant;

// Unique constraint on messageId enforces exactly-once processing at the database level.
// If an INSERT is attempted for a messageId that already exists, a DataIntegrityViolationException
// is thrown, which KafkaConsumerListener catches and routes to dead letter as DUPLICATE.
// To replay a failed message, delete this record first — the next delivery will then proceed normally.
@Entity
@Table(name = "received_record",
       uniqueConstraints = @UniqueConstraint(name = "uq_received_message_id", columnNames = "message_id"))
public class ReceivedRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false, unique = true)
    private String messageId;

    @Column(name = "interaction_id")
    private String interactionId;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    public Long getId() { return id; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getInteractionId() { return interactionId; }
    public void setInteractionId(String interactionId) { this.interactionId = interactionId; }

    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }
}
