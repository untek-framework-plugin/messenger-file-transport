<?php

namespace Untek\FrameworkPlugin\MessengerFileTransport\Infrastructure\Helpers;

use Symfony\Component\Filesystem\Filesystem;
use Untek\Core\FileSystem\Helpers\FindFileHelper;
use Untek\Core\Instance\Helpers\PropertyHelper;

class FileTransportHelper
{

    public static function clearQueue()
    {
        $directory = getenv('VAR_SHARED_DIRECTORY') . '/bus-file-db';
        (new Filesystem())->remove($directory);
    }

    public static function readFirstHandled(string $topic): object
    {
        $files = self::scan($topic, 'handled');
        if (count($files) > 1) {
            throw new RuntimeException('Many files!');
        }
        $content = file_get_contents($directory . '/' . $topic . '/handled/' . $files[0]);
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
        $directory = getenv('VAR_SHARED_DIRECTORY') . '/bus-file-db';
        return FindFileHelper::scanDir($directory . '/' . $topic . '/' . $type);
    }
}
