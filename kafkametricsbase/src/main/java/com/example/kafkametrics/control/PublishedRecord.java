package com.example.kafkametrics.control;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "published_record")
public class PublishedRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false)
    private String messageId;

    @Column(name = "interaction_id")
    private String interactionId;

    @Column(name = "published_at", nullable = false)
    private Instant publishedAt;

    public Long getId() { return id; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getInteractionId() { return interactionId; }
    public void setInteractionId(String interactionId) { this.interactionId = interactionId; }

    public Instant getPublishedAt() { return publishedAt; }
    public void setPublishedAt(Instant publishedAt) { this.publishedAt = publishedAt; }
}
