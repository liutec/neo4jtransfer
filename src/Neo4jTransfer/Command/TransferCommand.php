<?php

namespace Neo4jTransfer\Command;

use Neo4jTransfer\Neo4jTransfer;
use Symfony\Component\Console\Command\Command as BaseCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class TransferCommand extends BaseCommand
{
    protected function configure()
    {
        static::configureTransfer($this);
    }

    public static function configureTransfer(BaseCommand $command, $name='transfer', $output=true)
    {
        $command
            ->setName($name)
            ->setDescription('Clone all nodes and relations from one Neo4j Server database into another.')
        ;

        DumpCommand::configureSourceConnectionOptions($command);
        if ($output) {
            DumpCommand::configureOutputOptions($command);
        }
        ImportCommand::configureTargetConnectionOptions($command);
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        static::executeTransfer($input, $output);
    }

    public static function executeTransfer(InputInterface $input, OutputInterface $output)
    {
        list(
            $source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, $file, $clean,
            $transactional, $ignoredRelationProperties, $preserveIds
            ) = DumpCommand::makeReadArguments($input);
        list($target) = ImportCommand::makeWriteArguments($input);
        if ((!isset($file)) || ($file == 'default')) {
            $file = DumpCommand::makeDumpFileName($source->getHost(), $output);
        }
        if (isset($file)) {
            $file = fopen($file, 'w+');
        }
        Neo4jTransfer::transfer($source, $target, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize,
            $relationBatchSize, $clean, $transactional, $ignoredRelationProperties, $preserveIds, $file, $output);
        if (isset($file)) {
            fclose($file);
        }
    }
}