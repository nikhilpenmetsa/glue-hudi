#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { GlueStack } from '../lib/glue-stack';
import { PreReqStack } from '../lib/prereq-stack';

const app = new cdk.App();

const prereqStack = new PreReqStack(app, 'PreReqStack', {
  stackName : 'prereqStack',
  description : 'creates raw bucket, process bucket, a DynamoDB table'
});

const glue_stack = new GlueStack(app, 'GlueStack', {
  glueRoleGrantReadWrite: prereqStack.glueRoleGrantReadWrite,
  rawBucket: prereqStack.rawBucket,
  processedBucket: prereqStack.processedBucket,
  libraryBucket: prereqStack.libraryBucket,
  controlTable: prereqStack.controlTable,

});

