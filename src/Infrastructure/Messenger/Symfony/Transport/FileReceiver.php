<?php

namespace Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Messenger\Symfony\Transport;

use Untek\Framework\Messenger\Infrastructure\Messenger\Symfony\Stamp\TopicStamp;
use Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Messenger\Entities\MessageEntity;
use Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Persistence\FileStorage\Repository\FileRepository;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Untek\Core\Contract\Common\Exceptions\NotFoundException;

class FileReceiver implements ReceiverInterface
{

    public function __construct(
        protected SerializerInterface $serializer,
        protected FileRepository $fileRepository
    ) {
    }

    public function get(): iterable
    {
        try {
            $message = $this->fileRepository->findFirst();
            $envelope = $this->decode($message);
            return [$envelope];
        } catch (NotFoundException $e) {
            return [];
        }
    }

    public function ack(Envelope $envelope): void
    {
        /** @var TransportMessageIdStamp $transportMessageIdStamp */
        $transportMessageIdStamp = $envelope->last(TransportMessageIdStamp::class);
        $this->fileRepository->deleteById($transportMessageIdStamp->getId());
    }

    public function reject(Envelope $envelope): void
    {
        $this->ack($envelope);
    }

    protected function decode(MessageEntity $messageEntity): Envelope
    {
        $encodedEnvelope = $this->forgeEncodedEnvelope($messageEntity);
        $encodedEnvelope['headers'] = $this->cleanStampsFromHeaders($encodedEnvelope['headers']);
        $envelope = $this->serializer->decode($encodedEnvelope);
        $envelope = $this->addStampsFromKafkaMessage($envelope, $messageEntity);
        return $envelope;
    }

    protected function forgeEncodedEnvelope(
        MessageEntity $messageEntity
    ): array {
        return [
            'body' => $messageEntity->getBody(),
            'headers' => $messageEntity->getHeaders(),
        ];
    }

    protected function addStampsFromKafkaMessage(Envelope $envelope, MessageEntity $messageEntity): Envelope
    {
        $envelope = $envelope->with(new TransportMessageIdStamp($messageEntity->getId()));
        $envelope = $envelope->with(new TopicStamp($messageEntity->getTopic()));
//        $envelope = $envelope->with(new ConsumeMessageStamp($messageEntity));
        return $envelope;
    }

    /**
     * @param array
     * @return array
     */
    protected function cleanStampsFromHeaders(array $headers): array
    {
        foreach ($headers as $headerKey => $headerValue) {
            if (str_starts_with($headerKey, 'X-Message')) {
                unset($headers[$headerKey]);
            }
        }
        return $headers;
    }
}
