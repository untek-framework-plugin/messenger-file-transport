<?php

namespace Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Helpers;

use Symfony\Component\Filesystem\Filesystem;
use Untek\Core\FileSystem\Helpers\FindFileHelper;
use Untek\Core\Instance\Helpers\PropertyHelper;

class FileTransportHelper
{

    public static function clearQueue()
    {
        $directory = self::getBaseDirectory();
        (new Filesystem())->remove($directory);
    }

    public static function readFirstHandled(string $topic): object
    {
        $files = self::scan($topic, 'handled');
        if (count($files) > 1) {
            throw new RuntimeException('Many files!');
        }
        $content = file_get_contents(self::getDirectory($topic, 'handled') . '/' . $files[0]);
        $data = json_decode($content, true);
        $messageClass = $data['headers']['type'];
        $message = new $messageClass();
        $body = json_decode($data['body'], true);
        PropertyHelper::setAttributes($message, $body);
        return $message;
    }

    public static function waitHandleCount(string $topic): int
    {
        $files = self::scan($topic, 'queue');
        return count($files);
    }

    private static function scan(string $topic, string $type): array
    {
        return FindFileHelper::scanDir(self::getDirectory($topic, $type));
    }

    private static function getDirectory(string $topic, string $type): string
    {
        $directory = self::getBaseDirectory();
        return $directory . '/' . $topic . '/' . $type;
    }

    private static function getBaseDirectory(): string
    {
        return getenv('VAR_SHARED_DIRECTORY') . '/bus-file-db';
    }
}
