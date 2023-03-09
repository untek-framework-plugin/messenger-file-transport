<?php

namespace Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Messenger\Symfony\Transport;

use Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Messenger\Entities\MessageEntity;
use Untek\Framework\Messenger\Infrastructure\Messenger\Symfony\Stamp\TopicStamp;
use Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Persistence\FileStorage\Repository\FileRepository;
use longlang\phpkafka\Producer\ProduceMessage;
use longlang\phpkafka\Producer\Producer;
use longlang\phpkafka\Producer\ProducerConfig;
use longlang\phpkafka\Protocol\RecordBatch\RecordHeader;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class FileSender implements SenderInterface
{

    private string $topic;
    private string $broker;
    private string $clientId;

    public function __construct(
        private SerializerInterface $serializer,
        protected FileRepository $fileRepository
    ) {
    }

    public function setTopic(string $topic): void
    {
        $this->topic = $topic;
    }

    public function setBroker(string $broker): void
    {
        $this->broker = $broker;
    }

    public function setClientId(string $clientId): void
    {
        $this->clientId = $clientId;
    }

    public function send(Envelope $envelope): Envelope
    {
        $envelope = $this->addStamps($envelope);
        $produceMessage = $this->createMessageEntity($envelope);
        $this->fileRepository->create($produceMessage);
        return $envelope;
    }

    protected function createMessageEntity(Envelope $envelope): MessageEntity
    {
        $encodedEnvelope = $this->serializer->encode($envelope);
//        $headers = $this->headersToRecordHeaders($encodedEnvelope['headers']);
        /** @var TopicStamp $topicStamp */
        $topicStamp = $envelope->last(TopicStamp::class);
        /** @var TransportMessageIdStamp $transportMessageIdStamp */
        $transportMessageIdStamp = $envelope->last(TransportMessageIdStamp::class);

        $messageEntity = new MessageEntity();
        $messageEntity->setHeaders($encodedEnvelope['headers']);
        $messageEntity->setId($transportMessageIdStamp->getId());
        $messageEntity->setBody($encodedEnvelope['body']);
        $messageEntity->setTopic($topicStamp->getTopic());

        return $messageEntity;
    }

    protected function addStamps(Envelope $envelope): Envelope
    {
        $id = uniqid('', true);
        $envelope = $envelope->with(new TransportMessageIdStamp($id));
        /** @var TopicStamp $topicStamp */
        $topicStamp = $envelope->last(TopicStamp::class);
        if (empty($topicStamp)) {
            $envelope = $envelope->with(new TopicStamp($this->topic));
        }
        return $envelope;
    }
}
