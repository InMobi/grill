/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api.scheduler;

import org.apache.lens.api.error.InvalidStateTransitionException;

public enum SchedulerJobInstanceState
    implements StateTransitioner<SchedulerJobInstanceState, SchedulerJobInstanceEvent> {
  // repeating same operation will return the same state to ensure idempotent behavior.
  WAITING {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_CREATION:
        return this;
      case ON_CONDITIONS_MET:
        return SchedulerJobInstanceState.LAUNCHED;
      case ON_TIME_OUT:
        return SchedulerJobInstanceState.TIMED_OUT;
      case ON_RUN:
        return SchedulerJobInstanceState.RUNNING;
      case ON_SUCCESS:
        return SchedulerJobInstanceState.SUCCEEDED;
      case ON_FAILURE:
        return SchedulerJobInstanceState.FAILED;
      case ON_KILL:
        return SchedulerJobInstanceState.KILLED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  LAUNCHED {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_CONDITIONS_MET:
        return this;
      case ON_RUN:
        return SchedulerJobInstanceState.RUNNING;
      case ON_SUCCESS:
        return SchedulerJobInstanceState.SUCCEEDED;
      case ON_FAILURE:
        return SchedulerJobInstanceState.FAILED;
      case ON_KILL:
        return SchedulerJobInstanceState.KILLED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  RUNNING {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_RUN:
        return this;
      case ON_SUCCESS:
        return SchedulerJobInstanceState.SUCCEEDED;
      case ON_FAILURE:
        return SchedulerJobInstanceState.FAILED;
      case ON_KILL:
        return SchedulerJobInstanceState.KILLED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  FAILED {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_FAILURE:
        return this;
      case ON_RERUN:
        return SchedulerJobInstanceState.LAUNCHED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  SUCCEEDED {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_SUCCESS:
        return this;
      case ON_RERUN:
        return SchedulerJobInstanceState.LAUNCHED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  TIMED_OUT {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_TIME_OUT:
        return this;
      case ON_RERUN:
        return SchedulerJobInstanceState.WAITING;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  KILLED {
    @Override
    public SchedulerJobInstanceState nextTransition(SchedulerJobInstanceEvent event)
      throws InvalidStateTransitionException {
      switch (event) {
      case ON_KILL:
        return this;
      case ON_RERUN:
        return SchedulerJobInstanceState.LAUNCHED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobInstanceEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  }
}
