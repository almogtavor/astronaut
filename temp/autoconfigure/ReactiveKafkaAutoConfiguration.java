/*
 * Copyright 2012-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sky.autoconfigure;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.Map;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Apache Kafka.
 *
 * @author Almog Tavor
 * @since 1.5.0
 */
// TODO: replace with @AutoConfiguration
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({KafkaReceiver.class, KafkaSender.class})
@EnableConfigurationProperties(ReactiveKafkaProperties.class)
public class ReactiveKafkaAutoConfiguration {
    private final KafkaProperties kafkaProperties;
    private final ReactiveKafkaProperties reactiveKafkaProperties;

    public ReactiveKafkaAutoConfiguration(KafkaProperties kafkaProperties, ReactiveKafkaProperties reactiveKafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.reactiveKafkaProperties = reactiveKafkaProperties;
    }

    @Bean
    @ConditionalOnMissingBean(ReceiverOptions.class)
    public <K, V> ReceiverOptions<K, V> receiverOptions() {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
        properties.putAll(this.reactiveKafkaProperties.buildReceiverProperties());
        ReceiverOptions<K, V> receiverOptions = ReceiverOptions.<K, V>create(properties);
        if (this.reactiveKafkaProperties.getReceiver().getRevokeListeners() != null) {
            for (int i = 0; i < this.reactiveKafkaProperties.getReceiver().getRevokeListeners().size(); i++) {
                receiverOptions = receiverOptions.addRevokeListener(this.reactiveKafkaProperties.getReceiver().getRevokeListeners().get(i));
            }
        }
        if (this.reactiveKafkaProperties.getReceiver().getAssignListeners() != null) {
            for (int i = 0; i < this.reactiveKafkaProperties.getReceiver().getAssignListeners().size(); i++) {
                receiverOptions = receiverOptions.addAssignListener(this.reactiveKafkaProperties.getReceiver().getAssignListeners().get(i));
            }
        }
        if (this.reactiveKafkaProperties.getReceiver().getAtmostOnceCommitAheadSize() >= 0) {
            receiverOptions = receiverOptions.atmostOnceCommitAheadSize(this.reactiveKafkaProperties.getReceiver().getAtmostOnceCommitAheadSize());
        }
        Duration closeTimeout = this.reactiveKafkaProperties.getReceiver().getCloseTimeout();
        if (closeTimeout != null) {
            receiverOptions = receiverOptions.closeTimeout(closeTimeout);
        }
        if (this.reactiveKafkaProperties.getReceiver().getPollTimeout() != null) {
            receiverOptions = receiverOptions.pollTimeout(this.reactiveKafkaProperties.getReceiver().getPollTimeout());
        }
        if (this.reactiveKafkaProperties.getReceiver().getSchedulerSupplier() != null) {
            receiverOptions = receiverOptions.schedulerSupplier(this.reactiveKafkaProperties.getReceiver().getSchedulerSupplier());
        }
        if (this.reactiveKafkaProperties.getReceiver().getMaxDeferredCommits() >= 0) {
            receiverOptions = receiverOptions.maxDeferredCommits(this.reactiveKafkaProperties.getReceiver().getMaxDeferredCommits());
        }
        if (this.reactiveKafkaProperties.getReceiver().getMaxCommitAttempts() >= 0) {
            receiverOptions = receiverOptions.maxCommitAttempts(this.reactiveKafkaProperties.getReceiver().getMaxCommitAttempts());
        }
        if (this.reactiveKafkaProperties.getReceiver().getCommitBatchSize() >= 0) {
            receiverOptions = receiverOptions.commitBatchSize(this.reactiveKafkaProperties.getReceiver().getCommitBatchSize());
        }
        if (this.reactiveKafkaProperties.getReceiver().getCommitInterval() != null) {
            receiverOptions = receiverOptions.commitInterval(this.reactiveKafkaProperties.getReceiver().getCommitInterval());
        }
        if (this.reactiveKafkaProperties.getReceiver().getSubscribeTopics() != null) {
            receiverOptions = receiverOptions.subscription(this.reactiveKafkaProperties.getReceiver().getSubscribeTopics());
        } else {
            if (this.reactiveKafkaProperties.getReceiver().getSubscribePattern() != null) {
                receiverOptions = receiverOptions.subscription(this.reactiveKafkaProperties.getReceiver().getSubscribePattern());
            } else {
                throw new TopicsSubscriptionCreationException("No Kafka topics has been configured. Verify topics are configured by " +
                    "'spring.reactor.kafka.subscribe-topics' or 'spring.reactor.kafka.subscribe-pattern'");
            }
        }
        return receiverOptions;
    }

    @Bean
    @ConditionalOnMissingBean(SenderOptions.class)
    public <K, V> SenderOptions<K, V> senderOptions() {
        Map<String, Object> properties = kafkaProperties.buildProducerProperties();
        properties.putAll(this.reactiveKafkaProperties.buildSenderProperties());
        SenderOptions<K, V> senderOptions = SenderOptions.<K, V>create(properties);
        if (this.reactiveKafkaProperties.getSender().getScheduler() != null) {
            senderOptions = senderOptions.scheduler(this.reactiveKafkaProperties.getSender().getScheduler());
        }
        if (this.reactiveKafkaProperties.getSender().getCloseTimeout() != null) {
            senderOptions = senderOptions.closeTimeout(this.reactiveKafkaProperties.getSender().getCloseTimeout());
        }
        if (this.reactiveKafkaProperties.getSender().getMaxInFlight() >= 0) {
            senderOptions = senderOptions.maxInFlight(this.reactiveKafkaProperties.getSender().getMaxInFlight());
        }
        senderOptions = senderOptions.stopOnError(this.reactiveKafkaProperties.getSender().isStopOnError());
        return senderOptions;
    }

    static class TopicsSubscriptionCreationException extends BeanCreationException {

        TopicsSubscriptionCreationException(String message) {
            super(message);
        }

    }

}
