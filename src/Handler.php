<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\InsertMva;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client;
use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithClient;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;
use parallel\Runtime;

final class Handler extends BaseHandlerWithClient {
	/**
	 * Initialize the executor
	 *
	 * @param Payload $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

  /**
	 * Process the request
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(Runtime $runtime): Task {
		$this->manticoreClient->setPath($this->payload->path);
		// We may have seg fault so to avoid it we do encode decode trick to reduce memory footprint
		// for threaded function that runs in parallel
		$encodedPayload = gzencode(serialize($this->payload), 6);
		$taskFn = static function (string $encodedPayload, Client $manticoreClient): TaskResult {
			$payload = unserialize(gzdecode($encodedPayload));
			$query = "desc {$payload->table}";
			/** @var array{error?:string} */
			$descResult = $manticoreClient->sendRequest($query)->getResult();
			if (isset($descResult['error'])) {
				return TaskResult::withError($descResult['error']);
			}

			$columnCount = sizeof($descResult[0]['data']);
			$columnFnMap = [];
			/** @var array<array{data:array<array{Field:string,Type:string}>}> $descResult */
			foreach ($descResult[0]['data'] as $n => ['Field' => $field, 'Type' => $type]) {
				$columnFnMap[$n] = match ($type) {
					'mva', 'mva64' => function ($v) { return '(' . trim((string)$v, "'") . ')'; },
					default => function ($v) { return $v; },
				};
			}

			$query = "INSERT INTO `{$payload->table}` VALUES ";
			$total = (int)(sizeof($payload->values) / $columnCount);
			for ($i = 0; $i < $total; $i++) {
				$values = [];
				foreach ($columnFnMap as $n => $fn) {
					$pos = $i * $columnCount + $n;
					$values[] = $fn($payload->values[$pos]);
				}
				$queryValues = implode(', ', $values);
				$query .= "($queryValues),";
			}
			$query = trim($query, ', ');
			$insertResult = $manticoreClient->sendRequest($query)->getResult();
			/** @var array{error?:string} $insertResult */
			if (isset($insertResult['error'])) {
				return TaskResult::withError($insertResult['error']);
			}

			return TaskResult::raw($insertResult);
		};

		return Task::createInRuntime(
			$runtime, $taskFn, [$encodedPayload, $this->manticoreClient]
		)->run();
	}
}
