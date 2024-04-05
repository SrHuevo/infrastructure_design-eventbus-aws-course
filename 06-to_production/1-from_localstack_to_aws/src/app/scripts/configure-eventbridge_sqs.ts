import "reflect-metadata";

import {
	CreateEventBusCommand,
	EventBridgeClient,
	PutRuleCommand,
	PutTargetsCommand,
} from "@aws-sdk/client-eventbridge";
import { CreateQueueCommand, SQSClient } from "@aws-sdk/client-sqs";

import { DomainEvent } from "../../contexts/shared/domain/event/DomainEvent";
import { DomainEventSubscriber } from "../../contexts/shared/domain/event/DomainEventSubscriber";
import { container } from "../../contexts/shared/infrastructure/dependency_injection/diod.config";

type QueueConfig = {
	name: string;
	rulePattern: string[];
};

type RuleConfig = {
	name: string;
	eventBusName: string;
	rulePattern: string;
};

type TargetConfig = {
	eventBusName: string;
	ruleName: string;
	queueName: string[];
};

const eventBridgeClient = new EventBridgeClient({
	region: "us-east-1",
});

const sqsClient = new SQSClient({
	region: "us-east-1",
});

const subscribers = container
	.findTaggedServiceIdentifiers<DomainEventSubscriber<DomainEvent>>("subscriber")
	.map((id) => container.get(id));

const queues: QueueConfig[] = subscribers.map((subscriber) => ({
	name: subscriber.name().replaceAll(".", "-"),
	rulePattern: subscriber.subscribedTo().map((event) => event.eventName),
}));

async function main(): Promise<void> {
	const eventBusName = "codely.domain_events";

	await createEventBus(eventBusName);

	await Promise.all(extractRulesFromQueues(queues, eventBusName).map(createRule));

	await Promise.all(queues.map(createQueueAndDeadLetter));

	await Promise.all(extractTargetsFromQueues(queues, eventBusName).map(createTarget));
}

async function createEventBus(name: string): Promise<void> {
	await eventBridgeClient.send(new CreateEventBusCommand({ Name: name }));
}

async function createRule(rule: RuleConfig): Promise<void> {
	const eventPattern = rule.rulePattern.includes("*")
		? // ? `{"detail-type": [{"wildcard": "${rule.rulePattern}"}]}` // no en localstack
		  `{"detail-type": [{"prefix": "${rule.rulePattern.replace("*", "")}"}]}`
		: `{"detail-type": ["${rule.rulePattern}"]}`;

	await eventBridgeClient.send(
		new PutRuleCommand({
			Name: rule.name,
			EventBusName: rule.eventBusName,
			EventPattern: eventPattern,
		}),
	);
}

async function createQueueAndDeadLetter(queue: QueueConfig): Promise<void> {
	const deadLetterQueueName = `${queue.name}-dl`;

	await sqsClient.send(
		new CreateQueueCommand({
			QueueName: deadLetterQueueName,
		}),
	);

	await sqsClient.send(
		new CreateQueueCommand({
			QueueName: queue.name,
			Attributes: {
				RedrivePolicy: JSON.stringify({
					maxReceiveCount: "2",
					deadLetterTargetArn: `arn:aws:sqs:us-east-1:992382584577:${deadLetterQueueName}`,
				}),
				VisibilityTimeout: "3",
			},
		}),
	);
}

async function createTarget(target: TargetConfig): Promise<void> {
	await eventBridgeClient.send(
		new PutTargetsCommand({
			Rule: target.ruleName,
			EventBusName: target.eventBusName,
			Targets: target.queueName.map((queueName) => {
				return {
					Id: queueName,
					Arn: `arn:aws:sqs:us-east-1:992382584577:${queueName}`,
				};
			}),
		}),
	);
}

function extractRulesFromQueues(queues: QueueConfig[], eventBusName: string): RuleConfig[] {
	const patterns = queues.flatMap((queue) => queue.rulePattern)
	const uniquePatterns = new Set<string>(patterns);

	return Array.from(uniquePatterns).map((pattern) => ({
		name: formatRuleName(pattern),
		eventBusName,
		rulePattern: pattern,
	}));
}

function extractTargetsFromQueues(queues: QueueConfig[], eventBusName: string): TargetConfig[] {
	const targetsMap = queues.reduce((targetsMap, queue) => {
		return queue.rulePattern.reduce((targetsMap, pattern) => {
			const ruleName = formatRuleName(pattern);
			if (!targetsMap[ruleName])) {
				targetsMap[ruleName] = {
					eventBusName,
					ruleName,
					queueName: [queue.name],
				};
			} else {
				targetsMap[ruleName].queueName.push(queue.name);
			}
			return targetsMap;
		}, targetsMap);
	}, {} as Record<string, TargetConfig>);
	
	return Object.values(targetsMap);
}

function formatRuleName(rulePattern: string): string {
	return `rule-${rulePattern.replace("*", "all")}`;
}

main().catch(console.error);
