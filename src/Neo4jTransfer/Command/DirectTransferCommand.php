<?php

namespace Neo4jTransfer\Command;

use Neo4jTransfer\Neo4jTransfer;
use Symfony\Component\Console\Command\Command as BaseCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class DirectTransferCommand extends BaseCommand
{
    protected function configure()
    {
        TransferCommand::configureTransfer($this, 'direct', false);
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
            ) = DumpCommand::makeReadArguments($input, 300, 100, 150);
        list($target) = ImportCommand::makeWriteArguments($input);
        Neo4jTransfer::directTransfer($source, $target, $readBatchSize, $nodeBatchSize,
            $relationBatchSize, $ignoredRelationProperties, $preserveIds, $output);
    }
}