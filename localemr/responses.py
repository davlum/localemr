from __future__ import unicode_literals
import functools
from moto.emr.responses import ElasticMapReduceResponse, generate_boto3_response
from localemr.models import emr_backends
from localemr.common import parse_release_label


def validate_wrapper(func):
    @functools.wraps(func)
    def wrapper(self):
        release_label = self._get_param("ReleaseLabel")
        if release_label:
            parse_release_label(release_label)
        return func(self)
    return wrapper


# pylint: disable=abstract-method
class LocalElasticMapReduceResponse(ElasticMapReduceResponse):

    @property
    def backend(self):
        return emr_backends[self.region]

    @generate_boto3_response
    def cancel_steps(self):
        pass

    @generate_boto3_response("DescribeStep")
    def describe_step(self):
        cluster_id = self._get_param("ClusterId")
        step_id = self._get_param("StepId")
        step = self.backend.describe_step(cluster_id, step_id)
        template = self.response_template(DESCRIBE_STEP_TEMPLATE)
        return template.render(step=step)

    @generate_boto3_response("ListSteps")
    def list_steps(self):
        cluster_id = self._get_param("ClusterId")
        marker = self._get_param("Marker")
        step_ids = self._get_multi_param("StepIds.member")
        step_states = self._get_multi_param("StepStates.member")
        steps, marker = self.backend.list_steps(
            cluster_id, marker=marker, step_ids=step_ids, step_states=step_states
        )
        template = self.response_template(LIST_STEPS_TEMPLATE)
        return template.render(steps=steps, marker=marker)

    run_job_flow = validate_wrapper(ElasticMapReduceResponse.run_job_flow)


DESCRIBE_STEP_TEMPLATE = """<DescribeStepResponse xmlns="http://elasticmapreduce.amazonaws.com/doc/2009-03-31">
  <DescribeStepResult>
    <Step>
      <ActionOnFailure>{{ step.action_on_failure }}</ActionOnFailure>
      <Config>
        <Args>
          {% for arg in step.args %}
          <member>{{ arg | escape }}</member>
          {% endfor %}
        </Args>
        <Jar>{{ step.jar }}</Jar>
        <MainClass/>
        <Properties>
          {% for key, val in step.properties.items() %}
          <member>
            <key>{{ key }}</key>
            <value>{{ val | escape }}</value>
          </member>
          {% endfor %}
        </Properties>
      </Config>
      <Id>{{ step.id }}</Id>
      <Name>{{ step.name | escape }}</Name>
      <Status>
        <FailureDetails>
          <Reason>{{ step.failure_details.reason }}</Reason>
          <Message>{{ step.failure_details.message }}</Message>
          <LogFile>{{ step.failure_details.log_file }}</LogFile>
        </FailureDetails>
        <State>{{ step.state }}</State>
        <StateChangeReason>{{ step.state_change_reason }}</StateChangeReason>
        <Timeline>
          <CreationDateTime>{{ step.creation_datetime.isoformat() }}</CreationDateTime>
          {% if step.end_datetime is not none %}
          <EndDateTime>{{ step.end_datetime.isoformat() }}</EndDateTime>
          {% endif %}
          {% if step.ready_datetime is not none %}
          <StartDateTime>{{ step.start_datetime.isoformat() }}</StartDateTime>
          {% endif %}
        </Timeline>
      </Status>
    </Step>
  </DescribeStepResult>
  <ResponseMetadata>
    <RequestId>df6f4f4a-ed85-11dd-9877-6fad448a8419</RequestId>
  </ResponseMetadata>
</DescribeStepResponse>"""

LIST_STEPS_TEMPLATE = """<ListStepsResponse xmlns="http://elasticmapreduce.amazonaws.com/doc/2009-03-31">
  <ListStepsResult>
    <Steps>
      {% for step in steps %}
      <member>
        <ActionOnFailure>{{ step.action_on_failure }}</ActionOnFailure>
        <Config>
          <Args>
            {% for arg in step.args %}
            <member>{{ arg | escape }}</member>
            {% endfor %}
          </Args>
          <Jar>{{ step.jar | escape }}</Jar>
          <MainClass/>
          <Properties>
            {% for key, val in step.properties.items() %}
            <member>
              <key>{{ key }}</key>
              <value>{{ val | escape }}</value>
            </member>
            {% endfor %}
          </Properties>
        </Config>
        <Id>{{ step.id }}</Id>
        <Name>{{ step.name | escape }}</Name>
        <Status>
<!-- does not exist for botocore 1.4.28
          <FailureDetails>
            <Reason>{{ step.failure_details.reason }}</Reason>
            <Message>{{ step.failure_details.message }}</Message>
            <LogFile>{{ step.failure_details.log_file }}</LogFile>
          </FailureDetails>
-->
          <State>{{ step.state }}</State>
          <StateChangeReason>{{ step.state_change_reason }}</StateChangeReason>
          <Timeline>
            <CreationDateTime>{{ step.creation_datetime.isoformat() }}</CreationDateTime>
            {% if step.end_datetime is not none %}
            <EndDateTime>{{ step.end_datetime.isoformat() }}</EndDateTime>
            {% endif %}
            {% if step.ready_datetime is not none %}
            <StartDateTime>{{ step.start_datetime.isoformat() }}</StartDateTime>
            {% endif %}
          </Timeline>
        </Status>
      </member>
      {% endfor %}
    </Steps>
    {% if marker is not none %}
    <Marker>{{ marker }}</Marker>
    {% endif %}
  </ListStepsResult>
  <ResponseMetadata>
    <RequestId>df6f4f4a-ed85-11dd-9877-6fad448a8419</RequestId>
  </ResponseMetadata>
</ListStepsResponse>"""
