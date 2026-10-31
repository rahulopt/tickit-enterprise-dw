#!/usr/bin/env python3
"""Offline isolated AWS artifacts. Never calls AWS, pip, Docker or git."""
import argparse
import ast
import hashlib
import json
import re
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TABLES = ('category','venue','users','event','listing','sales')


def workflow():
    branches = []
    for table in TABLES:
        states = {
            'Identity': {'Type':'Pass','Parameters':{'request.$':'$','task_run_id.$':'States.UUID()'},'Next':'Submit'},
            'Submit': task('submit_branch','Choose',table),
            'Wait': {'Type':'Wait','Seconds':10,'Next':'Poll'},
            'Poll': task('poll_branch','Choose',table),
            'Choose': choice('Result','Failed','Wait'),
            'Result': {'Type':'Pass','Parameters':{'dataset':table,'descriptor.$':'$.result_envelope.payload.descriptor'},'End':True},
            'Failed': {'Type':'Fail','Error':'ETLFailed','Cause':table+' failed; watchdog must drain children.'}}
        branches.append({'StartAt':'Identity','States':states})
    states = {
        'Validate': {'Type':'Task','Resource':'arn:aws:states:::lambda:invoke',
            'Parameters':{'FunctionName':'{{CONTROLLER_ARN}}','Payload':{'action':'validate','request.$':'$'}},
            'OutputPath':'$.Payload','Next':'SixBranches','Catch':catch()},
        'SixBranches': {'Type':'Parallel','Branches':branches,'ResultPath':'$.branches','Next':'SubmitWarehouse','Catch':catch()},
        'SubmitWarehouse':task('submit_warehouse','WarehouseChoice'),
        'WaitWarehouse':{'Type':'Wait','Seconds':5,'Next':'PollWarehouse'},
        'PollWarehouse':task('poll_warehouse','WarehouseChoice'),
        'WarehouseChoice':choice('NotifySuccess','NotifyFailure','WaitWarehouse'),
        'NotifySuccess':notify('SUCCEEDED','Success'),
        'NotifyFailure':notify('FAILED','Failed'),
        'Success':{'Type':'Succeed'},
        'Failed':{'Type':'Fail','Error':'IsolatedIntegrationFailed','Cause':'Retain lease until trusted watchdog drains children.'}}
    # A failed SNS publish must not turn an unsuccessful run into success.
    return {'Comment':'Isolated manifest integration only; no task releases a lease.',
            'StartAt':'Validate','TimeoutSeconds':900,'States':states}


def catch():
    return [{'ErrorEquals':['States.ALL'],'ResultPath':'$.error','Next':'NotifyFailure'}]


def task(action,next_state,table=None):
    payload = {'action':action,'request.$':'$.request' if table else '$'}
    if table:
        payload.update({'dataset':table,'task_run_id.$':'$.task_run_id'})
    elif action == 'submit_warehouse':
        payload['branches.$'] = '$.branches'
    result = {'Type':'Task','Resource':'arn:aws:states:::lambda:invoke',
        'Parameters':{'FunctionName':'{{CONTROLLER_ARN}}','Payload':payload},
        'ResultSelector':{'payload.$':'$.Payload'},'ResultPath':'$.result_envelope','Next':next_state}
    if not table:
        result['Catch'] = catch()
    return result


def choice(success,failure,pending):
    return {'Type':'Choice','Choices':[
        {'Variable':'$.result_envelope.payload.status','StringEquals':'SUCCEEDED','Next':success},
        {'Variable':'$.result_envelope.payload.status','StringEquals':'RUNNING','Next':pending}],
        'Default':failure}


def notify(status,next_state):
    return {'Type':'Task','Resource':'arn:aws:states:::sns:publish',
        'Parameters':{'TopicArn':'{{SNS_ARN}}','Subject':'TICKIT isolated '+status,
                      'Message.$':'States.JsonToString($)'},'ResultPath':'$.notification',
        'Next':next_state,'Catch':[{'ErrorEquals':['States.ALL'],'Next':'Failed'}]}


def validate_workflow(flow):
    def check(block):
        states = block['States']
        assert block['StartAt'] in states
        targets = []
        for name,state in states.items():
            if 'Next' in state: targets.append(state['Next'])
            targets.extend(x['Next'] for x in state.get('Catch',[]))
            targets.extend(x['Next'] for x in state.get('Choices',[]))
            if 'Default' in state: targets.append(state['Default'])
            assert state['Type'] in ('Task','Parallel','Choice','Pass','Wait','Succeed','Fail')
            if state['Type'] == 'Task':
                assert not state.get('Retry'), 'Submission tasks must not auto-retry'
            for child in state.get('Branches',[]): check(child)
        assert all(target in states for target in targets)
        reachable, pending = set(), [block['StartAt']]
        while pending:
            name = pending.pop()
            if name in reachable: continue
            reachable.add(name); node = states[name]
            pending.extend([node['Next']] if 'Next' in node else [])
            pending.extend(x['Next'] for x in node.get('Catch',[]))
            pending.extend(x['Next'] for x in node.get('Choices',[]))
            pending.extend([node['Default']] if 'Default' in node else [])
        assert reachable == set(states), 'Unreachable workflow states'
    check(flow)
    assert len(flow['States']['SixBranches']['Branches']) == 6
    assert 'release_lease' not in json.dumps(flow)
    return True


def ref(name): return {'Ref':name}
def sub(value): return {'Fn::Sub':value}
def attr(name,key): return {'Fn::GetAtt':[name,key]}
def imp(name): return {'Fn::ImportValue':sub('${TestName}-'+name)}
def statement(actions,resources,**extra):
    return dict(Effect='Allow',Action=actions,Resource=resources,**extra)


def base():
    return {'AWSTemplateFormatVersion':'2010-09-09','Description':'TICKIT ISOLATED ONLY; deploy only after approval',
        'Parameters':{'TestName':{'Type':'String','AllowedPattern':'tickit-it-[a-z0-9-]{3,24}'},
                      'ProductionAccountId':{'Type':'String','AllowedPattern':'[0-9]{12}'}},
        'Conditions':{'IsolatedAccount':{'Fn::And':[
            {'Fn::Not':[{'Fn::Equals':[ref('AWS::AccountId'),ref('ProductionAccountId')]}]},
            {'Fn::Equals':[ref('AWS::Region'),'us-east-1']}]}},'Resources':{},'Outputs':{}}


def add(template,name,kind,properties,retain=False):
    template['Resources'][name]={'Type':kind,'Condition':'IsolatedAccount','Properties':properties}
    if retain:
        template['Resources'][name]['DeletionPolicy']='Retain'
        template['Resources'][name]['UpdateReplacePolicy']='Retain'


def output(template,name,value):
    template['Outputs'][name]={'Condition':'IsolatedAccount','Value':value,'Export':{'Name':sub('${TestName}-'+name)}}


def role(template,name,service,policies):
    add(template,name,'AWS::IAM::Role',{'RoleName':sub('${TestName}-'+name),
        'AssumeRolePolicyDocument':{'Version':'2012-10-17','Statement':[{
            'Effect':'Allow','Principal':{'Service':service},'Action':'sts:AssumeRole'}]},
        'Policies':[{'PolicyName':'isolated-only','PolicyDocument':{'Version':'2012-10-17','Statement':policies}}]})


def foundation():
    t=base()
    add(t,'Bucket','AWS::S3::Bucket',{'BucketName':sub('${TestName}-${AWS::AccountId}-${AWS::Region}'),
        'VersioningConfiguration':{'Status':'Enabled'},'BucketEncryption':{'ServerSideEncryptionConfiguration':[{'ServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]},
        'PublicAccessBlockConfiguration':{key:True for key in ('BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets')},
        'LifecycleConfiguration':{'Rules':[{'Id':'abort-incomplete','Status':'Enabled','AbortIncompleteMultipartUpload':{'DaysAfterInitiation':1}}]}},True)
    root=sub('${Bucket.Arn}/tickit-v2/batches/*')
    immutable=[sub('${Bucket.Arn}/tickit-v2/batches/*/'+path) for path in ('bronze/*','input-manifest.json','attempts/*/silver/*','attempts/*/output-manifest.json','attempts/*/warehouse/*','attempts/*/pinned-inputs/*')]+[sub('${Bucket.Arn}/artifacts/*')]
    add(t,'BucketPolicy','AWS::S3::BucketPolicy',{'Bucket':ref('Bucket'),'PolicyDocument':{'Version':'2012-10-17','Statement':[
        {'Sid':'TLS','Effect':'Deny','Principal':'*','Action':'s3:*','Resource':[attr('Bucket','Arn'),sub('${Bucket.Arn}/*')],'Condition':{'Bool':{'aws:SecureTransport':'false'}}},
        {'Sid':'NoBatchDelete','Effect':'Deny','Principal':'*','Action':['s3:DeleteObject','s3:DeleteObjectVersion'],'Resource':immutable},
        {'Sid':'RequireCreateOnly','Effect':'Deny','Principal':'*','Action':'s3:PutObject','Resource':immutable,
         'Condition':{'Null':{'s3:if-none-match':'true'},'Bool':{'s3:ObjectCreationOperation':'true'}}}
    ]}})
    add(t,'Ledger','AWS::DynamoDB::Table',{'TableName':sub('${TestName}-ledger'),'BillingMode':'PAY_PER_REQUEST',
        'AttributeDefinitions':[{'AttributeName':key,'AttributeType':'S'} for key in ('pk','queue_partition','queue_sort')],
        'KeySchema':[{'AttributeName':'pk','KeyType':'HASH'}],
        'GlobalSecondaryIndexes':[{'IndexName':'queued-batches','KeySchema':[{'AttributeName':'queue_partition','KeyType':'HASH'},{'AttributeName':'queue_sort','KeyType':'RANGE'}],'Projection':{'ProjectionType':'ALL'}}],
        'PointInTimeRecoverySpecification':{'PointInTimeRecoveryEnabled':True}},True)
    add(t,'DeadQueue','AWS::SQS::Queue',{'QueueName':sub('${TestName}-dead.fifo'),'FifoQueue':True,'MessageRetentionPeriod':1209600,'SqsManagedSseEnabled':True})
    add(t,'BatchQueue','AWS::SQS::Queue',{'QueueName':sub('${TestName}-batches.fifo'),'FifoQueue':True,'MessageRetentionPeriod':1209600,'VisibilityTimeout':60,'SqsManagedSseEnabled':True,
        'RedrivePolicy':{'deadLetterTargetArn':attr('DeadQueue','Arn'),'maxReceiveCount':5}})
    add(t,'Topic','AWS::SNS::Topic',{'TopicName':sub('${TestName}-notifications')})
    add(t,'DeliveryQueue','AWS::SQS::Queue',{'QueueName':sub('${TestName}-notifications'),'MessageRetentionPeriod':86400,'SqsManagedSseEnabled':True})
    add(t,'DeliveryPolicy','AWS::SQS::QueuePolicy',{'Queues':[ref('DeliveryQueue')],'PolicyDocument':{'Version':'2012-10-17','Statement':[
        {'Effect':'Allow','Principal':{'Service':'sns.amazonaws.com'},'Action':'sqs:SendMessage','Resource':attr('DeliveryQueue','Arn'),'Condition':{'ArnEquals':{'aws:SourceArn':ref('Topic')}}}]}})
    add(t,'Subscription','AWS::SNS::Subscription',{'TopicArn':ref('Topic'),'Protocol':'sqs','Endpoint':attr('DeliveryQueue','Arn'),'RawMessageDelivery':True})
    role(t,'CopyRole','redshift.amazonaws.com',[
        statement(['s3:GetObject'],sub('${Bucket.Arn}/tickit-v2/batches/*/attempts/*/warehouse/*')),
        statement(['s3:GetBucketLocation'],attr('Bucket','Arn')),
        statement(['s3:ListBucket'],attr('Bucket','Arn'),Condition={'StringLike':{'s3:prefix':['tickit-v2/batches/*/attempts/*/warehouse/*']}})])
    # Redshift Serverless also uses this role as a service principal.
    t['Resources']['CopyRole']['Properties']['AssumeRolePolicyDocument']['Statement'][0]['Principal']['Service']=['redshift.amazonaws.com','redshift-serverless.amazonaws.com']
    t['Resources']['CopyRole']['Properties']['AssumeRolePolicyDocument']['Statement'][0]['Condition']={'StringEquals':{'aws:SourceAccount':ref('AWS::AccountId')}}
    for name,value in {'Bucket':ref('Bucket'),'BucketArn':attr('Bucket','Arn'),'Table':ref('Ledger'),'TableArn':attr('Ledger','Arn'),
        'QueueUrl':ref('BatchQueue'),'QueueArn':attr('BatchQueue','Arn'),'TopicArn':ref('Topic'),'DeliveryQueueUrl':ref('DeliveryQueue'),'CopyRoleArn':attr('CopyRole','Arn')}.items(): output(t,name,value)
    return t


def warehouse_template():
    t=base()
    add(t,'Vpc','AWS::EC2::VPC',{'CidrBlock':'10.77.0.0/16','EnableDnsSupport':True,'EnableDnsHostnames':True})
    for index in range(3):
        add(t,'Subnet'+str(index),'AWS::EC2::Subnet',{'VpcId':ref('Vpc'),'CidrBlock':f'10.77.{index}.0/24',
            'AvailabilityZone':{'Fn::Select':[index,{'Fn::GetAZs':''}]},'MapPublicIpOnLaunch':False})
    add(t,'SecurityGroup','AWS::EC2::SecurityGroup',{'GroupDescription':'Isolated warehouse: no inbound network connections','VpcId':ref('Vpc'),
        'SecurityGroupEgress':[{'IpProtocol':'-1','CidrIp':'0.0.0.0/0','Description':'Service connectivity; no IGW or NAT provisioned'}]})
    add(t,'Namespace','AWS::RedshiftServerless::Namespace',{'NamespaceName':sub('${TestName}-ns'),'DbName':'tickit_it','ManageAdminPassword':True,
        'AdminUsername':'tickit_test_admin','IamRoles':[imp('CopyRoleArn')],'DefaultIamRoleArn':imp('CopyRoleArn')})
    add(t,'Workgroup','AWS::RedshiftServerless::Workgroup',{'WorkgroupName':sub('${TestName}-wg'),'NamespaceName':ref('Namespace'),
        'BaseCapacity':8,'MaxCapacity':8,'PubliclyAccessible':False,'EnhancedVpcRouting':False,
        'SubnetIds':[ref('Subnet'+str(i)) for i in range(3)],'SecurityGroupIds':[ref('SecurityGroup')],
        'ConfigParameters':[{'ParameterKey':'max_query_execution_time','ParameterValue':'120'}]})
    output(t,'WorkgroupName',sub('${TestName}-wg')); output(t,'WorkgroupArn',attr('Workgroup','Workgroup.WorkgroupArn'))
    output(t,'NamespaceName',ref('Namespace')); output(t,'Database','tickit_it')
    return t


def runtime():
    t=base()
    bucket=imp('Bucket'); bucketarn=imp('BucketArn'); tablearn=imp('TableArn')
    batcharn={'Fn::Join':['',[bucketarn,'/tickit-v2/batches/*']]}
    artifactarn={'Fn::Join':['',[bucketarn,'/artifacts/*']]}
    def log_policy(): return statement(['logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents'],[
        sub('arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/${TestName}-*:*'),
        sub('arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/${TestName}/glue*:*')])
    ledger_read=statement(['dynamodb:GetItem'],tablearn)
    ledger_write=statement(['dynamodb:GetItem','dynamodb:PutItem'],tablearn)
    reads=statement(['s3:GetObject','s3:GetObjectVersion'],[batcharn,artifactarn])
    jobarns=[sub('arn:${AWS::Partition}:glue:${AWS::Region}:${AWS::AccountId}:job/${TestName}-'+name) for name in TABLES[2:]]
    functionarns=[sub('arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${TestName}-'+name) for name in TABLES[:2]]
    role(t,'EtlRole',['lambda.amazonaws.com','glue.amazonaws.com'],[log_policy(),ledger_read,reads,
        statement(['s3:GetBucketLocation'],bucketarn),
        statement(['s3:ListBucket'],bucketarn,Condition={'StringLike':{'s3:prefix':['tickit-v2/batches/*/attempts/*/scratch/*','tickit-v2/batches/*/attempts/*/pinned-inputs/*']}}),
        statement(['s3:PutObject','s3:AbortMultipartUpload','s3:ListMultipartUploadParts'],[
            {'Fn::Join':['',[bucketarn,'/tickit-v2/batches/*/attempts/*/'+path+'/*']]} for path in ('silver','scratch','pinned-inputs')]),
        statement(['s3:DeleteObject'],{'Fn::Join':['',[bucketarn,'/tickit-v2/batches/*/attempts/*/scratch/*']]})])
    # NoBatchDelete policy intentionally exempts scratch below, where Spark's
    # committer must rename/delete temporary files; final keys remain protected.
    role(t,'ControllerRole','lambda.amazonaws.com',[log_policy(),ledger_write,reads,
        statement(['s3:PutObject'],[{'Fn::Join':['',[bucketarn,'/tickit-v2/batches/*/attempts/*/'+p]]} for p in ('output-manifest.json','warehouse/*')]),
        statement(['glue:StartJobRun','glue:GetJobRun'],jobarns),statement(['lambda:InvokeFunction'],functionarns),
        statement(['redshift-data:ExecuteStatement','redshift-serverless:GetCredentials'],imp('WorkgroupArn')),
        statement(['redshift-data:DescribeStatement'],'*')])
    ledger_query=statement(['dynamodb:Query'],{'Fn::Join':['',[tablearn,'/index/queued-batches']]})
    role(t,'IngressRole','lambda.amazonaws.com',[log_policy(),ledger_write,reads,
        statement(['sqs:SendMessage'],imp('QueueArn'))])
    role(t,'DispatcherRole','lambda.amazonaws.com',[log_policy(),ledger_write,ledger_query,
        statement(['sqs:SendMessage','sqs:ReceiveMessage','sqs:DeleteMessage'],imp('QueueArn')),
        statement(['states:StartExecution','states:DescribeStateMachine'],sub('arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:stateMachine:${TestName}-workflow')),
        statement(['states:DescribeExecution'],sub('arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:execution:${TestName}-workflow:*')),
        statement(['glue:GetJobRun'],jobarns),statement(['redshift-data:DescribeStatement'],'*')])
    role(t,'WorkflowRole','states.amazonaws.com',[statement(['lambda:InvokeFunction'],sub('arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${TestName}-controller')),
        statement(['sns:Publish'],imp('TopicArn'))])
    add(t,'SdkLayer','AWS::Lambda::LayerVersion',{'LayerName':sub('${TestName}-sdk'),'CompatibleRuntimes':['python3.11'],
        'Content':{'S3Bucket':bucket,'S3Key':'artifacts/sdk-layer.zip'}})
    add(t,'DataLayer','AWS::Lambda::LayerVersion',{'LayerName':sub('${TestName}-data'),'CompatibleRuntimes':['python3.11'],
        'Content':{'S3Bucket':bucket,'S3Key':'artifacts/data-layer.zip'}})
    env={'TICKIT_BUCKET':bucket,'ADMISSION_TABLE':imp('Table'),'BATCH_QUEUE_URL':imp('QueueUrl'),
         'TICKIT_PIPELINE_ID':ref('TestName')}
    for name,module,rolename,timeout in [('category','category_etl','EtlRole',60),('venue','venue_etl','EtlRole',60),
        ('controller','isolated_controller','ControllerRole',120),('ingress','s3_trigger','IngressRole',30),('dispatcher','batch_dispatcher','DispatcherRole',45)]:
        variables=dict(env)
        if name=='controller':
            variables.update({'REDSHIFT_WORKGROUP':imp('WorkgroupName'),'REDSHIFT_DATABASE':'tickit_it',
                              'REDSHIFT_COPY_ROLE':imp('CopyRoleArn')})
        if name=='dispatcher':variables['STATE_MACHINE_ARN']=sub('arn:${AWS::Partition}:states:${AWS::Region}:${AWS::AccountId}:stateMachine:${TestName}-workflow')
        add(t,name.title()+'Function','AWS::Lambda::Function',{'FunctionName':sub('${TestName}-'+name),'Runtime':'python3.11','Architectures':['x86_64'],
            'Handler':module+'.lambda_handler','Role':attr(rolename,'Arn'),'Timeout':timeout,'MemorySize':512,'ReservedConcurrentExecutions':1 if name=='dispatcher' else 6,
            'Code':{'S3Bucket':bucket,'S3Key':'artifacts/'+name+'.zip'},'Layers':[ref('SdkLayer')]+([ref('DataLayer')] if name in TABLES[:2] else []),
            'Environment':{'Variables':variables}})
        add(t,name.title()+'Logs','AWS::Logs::LogGroup',{'LogGroupName':sub('/aws/lambda/${TestName}-'+name),'RetentionInDays':1})
    for suffix in ('error','output'):
        add(t,'Glue'+suffix.title()+'Logs','AWS::Logs::LogGroup',{'LogGroupName':sub('/${TestName}/glue/'+suffix),'RetentionInDays':1})
    for name,script in [('users','users_etl.py'),('event','events_etl.py'),('listing','listings_etl.py'),('sales','sales_etl.py')]:
        add(t,name.title()+'Job','AWS::Glue::Job',{'Name':sub('${TestName}-'+name),'Role':attr('EtlRole','Arn'),'GlueVersion':'5.0',
            'Command':{'Name':'glueetl','PythonVersion':'3','ScriptLocation':{'Fn::Join':['',['s3://',bucket,'/artifacts/'+script]]}},
            'WorkerType':'G.1X','NumberOfWorkers':2,'Timeout':5,'MaxRetries':0,'JobRunQueuingEnabled':False,
            'ExecutionProperty':{'MaxConcurrentRuns':1},'DefaultArguments':{
                '--extra-py-files':{'Fn::Join':['',['s3://',bucket,'/artifacts/tickit-control.zip']]},
                '--additional-python-modules':{'Fn::Join':['',['s3://',bucket,'/artifacts/glue-dependencies.gluewheels.zip']]},
                '--python-modules-installer-option':'--no-index','--job-bookmark-option':'job-bookmark-disable',
                '--custom-logGroup-prefix':sub('/${TestName}/glue'),
                '--conf':'spark.sql.session.timeZone=UTC --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS --conf spark.sql.ansi.enabled=true --conf spark.sql.shuffle.partitions=2'}})
    flow=workflow()
    def bind(value):
        if isinstance(value,dict):return {k:bind(v) for k,v in value.items()}
        if isinstance(value,list):return [bind(v) for v in value]
        return attr('ControllerFunction','Arn') if value=='{{CONTROLLER_ARN}}' else imp('TopicArn') if value=='{{SNS_ARN}}' else value
    add(t,'Workflow','AWS::StepFunctions::StateMachine',{'StateMachineName':sub('${TestName}-workflow'),'StateMachineType':'STANDARD',
        'RoleArn':attr('WorkflowRole','Arn'),'Definition':bind(flow)})
    add(t,'Watchdog','AWS::Events::Rule',{'Name':sub('${TestName}-watchdog'),'ScheduleExpression':'rate(1 minute)','State':'DISABLED',
        'Targets':[{'Id':'dispatcher','Arn':attr('DispatcherFunction','Arn')}]})
    add(t,'WatchdogPermission','AWS::Lambda::Permission',{'Action':'lambda:InvokeFunction','FunctionName':ref('DispatcherFunction'),
        'Principal':'events.amazonaws.com','SourceArn':attr('Watchdog','Arn')})
    add(t,'IngressPermission','AWS::Lambda::Permission',{'Action':'lambda:InvokeFunction','FunctionName':ref('IngressFunction'),
        'Principal':'s3.amazonaws.com','SourceAccount':ref('AWS::AccountId'),'SourceArn':bucketarn})
    output(t,'WorkflowArn',ref('Workflow'));output(t,'ControllerRoleArn',attr('ControllerRole','Arn'));output(t,'IngressArn',attr('IngressFunction','Arn'))
    return t


def write_zip(path, entries):
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as archive:
        for name,source in sorted(entries.items()):
            info=zipfile.ZipInfo(name,(2026,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
            archive.writestr(info,source.read_bytes())


def prepare(destination):
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    flow=workflow()
    validate_workflow(flow)
    for name,document in [('workflow',flow),('foundation',foundation()),('warehouse',warehouse_template()),('runtime',runtime())]:
        (destination/(name+'.template.json')).write_text(json.dumps(document,indent=2)+'\n')
    sources={str(p.relative_to(ROOT)):p for p in (ROOT/'tickit_control').glob('*.py')}
    for path in sources.values():ast.parse(path.read_text())
    write_zip(destination/'tickit-control.zip',sources)
    for name,module in [('category','category_etl'),('venue','venue_etl'),('controller','isolated_controller'),('ingress','s3_trigger'),('dispatcher','batch_dispatcher')]:
        write_zip(destination/(name+'.zip'),dict(sources,**{module+'.py':ROOT/'lambda'/(module+'.py')}))
    for script in (ROOT/'glue').glob('*_etl.py'):
        (destination/script.name).write_bytes(script.read_bytes())
    artifact_names={name+'.zip' for name in ('category','venue','controller','ingress','dispatcher','tickit-control','sdk-layer','data-layer')}
    artifact_names.update(name+'.template.json' for name in ('workflow','foundation','warehouse','runtime'))
    artifact_names.update(p.name for p in (ROOT/'glue').glob('*_etl.py'))
    artifact_names.update(('glue-dependencies.gluewheels.zip','wheel-lock.json','layer-sizes.json'))
    checksums={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(destination.iterdir()) if p.is_file() and p.name in artifact_names}
    (destination/'sql').mkdir(exist_ok=True)
    for name in ('01_create_schemas.sql','02_stage_tables.sql','03_dim_tables.sql','04_fact_tables.sql','05_load_dim_date.sql','06_stored_procedures.sql','09_manifest_warehouse.sql','10_manifest_permissions.sql'):
        source=ROOT/'sql'/name
        (destination/'sql'/name).write_bytes(source.read_bytes())
        checksums['sql/'+name]=hashlib.sha256(source.read_bytes()).hexdigest()
    (destination/'SHA256SUMS.json').write_text(json.dumps(checksums,indent=2)+'\n')
    return checksums


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,default=ROOT/'tests/isolated/build')
    print(json.dumps(prepare(parser.parse_args().out),indent=2))
