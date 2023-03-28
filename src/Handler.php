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

		$taskFn = static function (Payload $payload, Client $manticoreClient): TaskResult {
			$query = "desc {$payload->table}";
			/** @var array{error?:string} */
			$descResult = $manticoreClient->sendRequest($query)->getResult();
			if (isset($descResult['error'])) {
				return TaskResult::withError($descResult['error']);
			}

			/** @var array<array{data:array<array{Field:string,Type:string}>}> $descResult */
			$values = [];
			foreach ($descResult[0]['data'] as $n => ['Field' => $field, 'Type' => $type]) {
				$values[] = match ($type) {
					'mva64' => '(' . trim((string)$payload->values[$n], "'") . ')',
					default => $payload->values[$n],
				};
			}

			$queryValues = implode(', ', $values);
			$query = "INSERT INTO `{$payload->table}` VALUES ($queryValues)";

			$insertResult = $manticoreClient->sendRequest($query)->getResult();
			/** @var array{error?:string} $insertResult */
			if (isset($insertResult['error'])) {
				return TaskResult::withError($insertResult['error']);
			}
			return TaskResult::raw($insertResult);
		};

		return Task::createInRuntime(
			$runtime, $taskFn, [$this->payload, $this->manticoreClient]
		)->run();
	}
}
