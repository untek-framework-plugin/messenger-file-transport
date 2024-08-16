<?php

namespace Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Persistence\FileStorage\Repository;

use Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Messenger\Entities\MessageEntity;
use Untek\Core\Contract\Common\Exceptions\NotFoundException;
use Untek\Core\FileSystem\Helpers\FileHelper;
use Untek\Core\FileSystem\Helpers\FileStorageHelper;
use Untek\Core\FileSystem\Helpers\FindFileHelper;
use Untek\Core\Instance\Helpers\PropertyHelper;

class FileRepository
{

    public function __construct(private ?string $topic = null)
    {
    }

    public function getEntityClass(): string
    {
        return MessageEntity::class;
    }

    protected function getFileExtension(): string
    {
        return 'json';
    }

    protected function getDirectoryWithNewTopics(?string $topic = null): string
    {
        $topic = $topic ?: $this->topic;
        return $this->getDirectory() . '/' . $topic . '/queue';
    }

    protected function getDirectoryWithHandledTopics(?string $topic = null): string
    {
        $topic = $topic ?: $this->topic;
        return $this->getDirectory() . '/' . $topic . '/handled';
    }

    protected function getDirectory(): string
    {
        return getenv('VAR_SHARED_DIRECTORY') . '/bus-file-db';
    }

    public function create(MessageEntity $entity): void
    {
        $topic = $entity->getTopic() ?: $this->topic;
        $item = PropertyHelper::toArray($entity);
        $queueDirectory = $this->getDirectoryWithNewTopics($topic);
        $fileName = $queueDirectory . '/' . microtime(true) . '.' . $this->getFileExtension();
        $this->save($fileName, $item);
    }

    private function save($fileName, $item): void
    {
        FileStorageHelper::touchDirectory(dirname($fileName));
        $json = json_encode($item, JSON_PRETTY_PRINT);
        file_put_contents($fileName, $json);
    }

    private function load($fileName): array
    {
        $json = file_get_contents($fileName);
        $item = json_decode($json, JSON_OBJECT_AS_ARRAY);
        return $item;
    }

    public function findFirst(): MessageEntity
    {
        $directory = $this->getDirectoryWithNewTopics();
        $files = FindFileHelper::scanDir($directory);
        rsort($files);
        foreach ($files as $file) {
            $fileName = $directory . '/' . $file;
            $item = $this->load($fileName);
            if ($item['topic'] == $this->topic) {
                /** @var MessageEntity $messageEntity */
                $messageEntity = new MessageEntity();
                PropertyHelper::setAttributes($messageEntity, $item);
                return $messageEntity;
            }
        }
        throw new NotFoundException();
    }

    public function deleteById(string $id): void
    {
        $directory = $this->getDirectoryWithNewTopics();
        $files = FindFileHelper::scanDir($directory);
        rsort($files);
        foreach ($files as $file) {
            $fileName = $directory . '/' . $file;
            $item = $this->load($fileName);
            if ($item['topic'] == $this->topic && $item['id'] == $id) {
                FileHelper::createDirectory($this->getDirectoryWithHandledTopics());
                rename($fileName, $this->getDirectoryWithHandledTopics() . '/' . $file);
                return;
            }
        }
        throw new NotFoundException();
    }
}
