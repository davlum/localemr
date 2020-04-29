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