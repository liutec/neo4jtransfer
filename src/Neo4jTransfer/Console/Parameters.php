<?php

namespace Neo4jTransfer\Console;


class Parameters
{
    protected $inputFileName = null;
    protected $outputFileName = null;
    protected $verbose = false;
    protected $noInteraction = false;
    protected $versionMajor = 0;
    protected $versionMinor = 1;
    protected $versionRevision = 1;

    public function parse($argc, array $argv)
    {
        $mapping = array(
            'output' => &$this->outputFileName,
            'verbose' => &$this->verbose,
            'no-interaction' => &$this->noInteraction,
        );

        if ($argc < 2) {
            $this->showHelp();
            fwrite(STDERR, 'No input file specified.');
            exit(1);
        }
        $this->setInputFileName($argv[1]);
    }

    /**
     * The version string.
     *
     * @return string
     */
    public function getVersion()
    {
        return sprintf('%d.%d.%d', $this->versionMajor, $this->versionMinor, $this->versionRevision);
    }

    /**
     * Print the help.
     */
    public function showHelp()
    {
        $msgs = array(
            sprintf('Neo4j Transfer v%s', $this->getVersion()),
            'neo4jtransfer.phar ',
            '',
            'Options:',
            '--output destination file',
            'Saves the result in the given file name.',
            '',
            '--verbose',
            'Prints extended informations.',
            '',
            '--no-interaction',
            'Does not require any user interaction.',
            'exit(1) and error message to stderr if user input is required.',
        );
        foreach ($msgs as $msg) {
            echo $msg . "\n";
        }
    }

    /**
     * Check if input file name was set.
     *
     * @return boolean
     */
    public function hasInputFileName()
    {
        return isset($this->inputFileName);
    }

    /**
     * Get input file name.
     *
     * @return string|null
     */
    public function getInputFileName()
    {
        if (!$this->hasInputFileName()) {
            return null;
        }
        return $this->inputFileName;
    }

    /**
     * Set input file name.
     *
     * @param string $value The new value.
     * @return $this
     */
    public function setInputFileName($value)
    {
        $this->inputFileName = $value;
        return $this;
    }

    /**
     * Check if output file name was set.
     *
     * @return boolean
     */
    public function hasOutputFileName()
    {
        return isset($this->outputFileName);
    }

    /**
     * Get output file name.
     *
     * @return string|null
     */
    public function getOutputFileName()
    {
        if (!$this->hasOutputFileName()) {
            return null;
        }
        return $this->outputFileName;
    }

    /**
     * Set output file name.
     *
     * @param string $value The new value.
     * @return $this
     */
    public function setOutputFileName($value)
    {
        $this->outputFileName = $value;
        return $this;
    }

    /**
     * Get verbose.
     *
     * @return boolean
     */
    public function getVerbose()
    {
        return $this->verbose;
    }

    /**
     * Set verbose.
     *
     * @param boolean $value The new value.
     * @return $this
     */
    public function setVerbose($value)
    {
        $this->verbose = $value;
        return $this;
    }

    /**
     * Get no interaction.
     *
     * @return boolean
     */
    public function getNoInteraction()
    {
        return $this->noInteraction;
    }

    /**
     * Set no interaction.
     *
     * @param boolean $value The new value.
     * @return $this
     */
    public function setNoInteraction($value)
    {
        $this->noInteraction = $value;
        return $this;
    }
}