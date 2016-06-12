<?php
namespace Neo4jTransfer;

use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class Builder
{
    private $version;
    private $branchAliasVersion = '';
    private $versionDate;

    /**
     * Generates the phar file for Neo4j Transfer
     *
     * @param string $pharFile The full path to the file to create
     * @throws \RuntimeException
     */
    public function build($pharFile = 'neo4jtransfer.phar')
    {
        echo sprintf("Building %s...\n", $pharFile);
        if (file_exists($pharFile)) {
            unlink($pharFile);
        }

        $process = new Process('git log --pretty="%H" -n1 HEAD', __DIR__);
        if ($process->run() != 0) {
            throw new \RuntimeException('Problem running "git log" to get version.');
        }
        $this->version = trim($process->getOutput());

        $process = new Process('git log -n1 --pretty=%ci HEAD', __DIR__);
        if ($process->run() != 0) {
            throw new \RuntimeException('Problem running "git log" to get release date.');
        }
        $date = new \DateTime(trim($process->getOutput()));
        $date->setTimezone(new \DateTimeZone('UTC'));
        $this->versionDate = $date->format('Y-m-d H:i:s');

        $process = new Process('git describe --tags --exact-match HEAD');
        if ($process->run() == 0) {
            $this->version = trim($process->getOutput());
        } else {
            // get branch-alias defined in composer.json for dev-master (if any)
            $composerFile = __DIR__.'/../../composer.json';
            $composerFileData = json_decode($composerFile, true);
            if (isset($composerFileData['extra']['branch-alias']['dev-master'])) {
                $this->branchAliasVersion = $composerFileData['extra']['branch-alias']['dev-master'];
            }
        }

        $phar = new \Phar($pharFile, 0, $pharFile);
        $phar->setSignatureAlgorithm(\Phar::SHA1);
        $phar->startBuffering();

        $finder = new Finder();
        $finder->files()
            ->ignoreVCS(true)
            ->name('*.php')
            ->notName('Builder.php')
            ->in(__DIR__ . '/..')
        ;
        foreach ($finder as $file) {
            $this->addFile($phar, $file);
        }

        $finder = new Finder();
        $finder->files()
            ->ignoreVCS(true)
            ->name('*.php')
            ->name('LICENSE')
            ->exclude('Tests')
            ->exclude('tests')
            ->exclude('docs')
            ->in(__DIR__ . '/../../vendor/symfony/')
        ;
        foreach ($finder as $file) {
            $this->addFile($phar, $file);
        }

        $finder = new Finder();
        $finder->files()
            ->ignoreVCS(true)
            ->name('*.php')
            ->name('LICENSE')
            ->exclude('examples')
            ->exclude('tests')
            ->in(__DIR__ . '/../../vendor/everyman/')
        ;
        foreach ($finder as $file) {
            $this->addFile($phar, $file);
        }

        $finder = new Finder();
        $finder->files()
            ->ignoreVCS(true)
            ->name('*.php')
            ->exclude('*.json')
            ->in(__DIR__ . '/../../vendor/composer/')
        ;
        foreach ($finder as $file) {
            $this->addFile($phar, $file);
        }
        $finder = new Finder();
        $finder->files()
            ->ignoreVCS(true)
            ->name('autoload.php')
            ->in(__DIR__ . '/../../vendor/')
        ;
        foreach ($finder as $file) {
            $this->addFile($phar, $file);
        }
        $this->addNeo4jTransferBin($phar);
        $phar->setStub($this->getStub());
        $phar->stopBuffering();
        $phar->compressFiles(\Phar::GZ);
        unset($phar);
        
        chmod($pharFile, 0755);

        echo "Build was successful.\n";
    }

    /**
     * @param \Phar $phar
     * @param \SplFileInfo $file
     * @param bool $strip
     */
    private function addFile($phar, $file, $strip = true)
    {
        $path = strtr(str_replace(dirname(dirname(__DIR__)).DIRECTORY_SEPARATOR, '', $file->getRealPath()), '\\', '/');
        $content = file_get_contents($file);
        if ($strip) {
            $content = $this->stripWhitespace($content);
        } elseif ('LICENSE' === basename($file)) {
            $content = "\n".$content."\n";
        }
        if ($path === 'src/Neo4jTransfer/Neo4jTransfer.php') {
            $searchReplace = array(
                '@version@' => $this->version,
                '@branch_alias_version@' => $this->branchAliasVersion,
                '@release_date@' => $this->versionDate,
            );
            $content = str_replace(array_keys($searchReplace), array_values($searchReplace), $content);
        }
        echo sprintf("Adding %s.\n", $path);
        $phar->addFromString($path, $content);
    }

    /**
     * @param \Phar $phar
     */
    private function addNeo4jTransferBin($phar)
    {
        $content = file_get_contents(__DIR__ . '/../../bin/neo4jtransfer');
        $content = preg_replace('{^#!/usr/bin/env php\s*}', '', $content);
        $path = 'bin/neo4jtransfer';
        echo sprintf("Adding %s.\n", $path);
        $phar->addFromString($path, $content);
    }

    /**
     * Removes whitespace from a PHP source string while preserving line numbers.
     *
     * @param  string $source A PHP string
     * @return string The PHP string with the whitespace removed
     */
    private function stripWhitespace($source)
    {
        if (!function_exists('token_get_all')) {
            return $source;
        }
        $output = '';
        foreach (token_get_all($source) as $token) {
            if (is_string($token)) {
                $output .= $token;
            } elseif (in_array($token[0], array(T_COMMENT, T_DOC_COMMENT))) {
                $output .= str_repeat("\n", substr_count($token[1], "\n"));
            } elseif (T_WHITESPACE === $token[0]) {
                // reduce wide spaces
                $whitespace = preg_replace('{[ \t]+}', ' ', $token[1]);
                // normalize newlines to \n
                $whitespace = preg_replace('{(?:\r\n|\r|\n)}', "\n", $whitespace);
                // trim leading spaces
                $whitespace = preg_replace('{\n +}', "\n", $whitespace);
                $output .= $whitespace;
            } else {
                $output .= $token[1];
            }
        }
        return $output;
    }

    private function getStub()
    {
        return <<<'EOF'
#!/usr/bin/env php
<?php
Phar::mapPhar('neo4jtransfer.phar');
require 'phar://neo4jtransfer.phar/bin/neo4jtransfer';
__HALT_COMPILER();
EOF;
    }
}