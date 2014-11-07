/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.events.*;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.api.query.QueryFailed;
import org.apache.lens.server.api.query.QuerySuccess;
import org.apache.lens.server.api.query.QueuePositionChange;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

/**
 * The Class TestEventService.
 */
@Test(groups = "unit-test")
public class TestEventService {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(TestEventService.class);

  /** The service. */
  EventServiceImpl service;

  /** The generic event listener. */
  GenericEventListener genericEventListener;

  /** The failed listener. */
  MockFailedListener failedListener;

  /** The queue position change listener. */
  MockQueuePositionChange queuePositionChangeListener;

  /** The ended listener. */
  MockEndedListener endedListener;

  /** The latch. */
  CountDownLatch latch;

  /**
   * The listener interface for receiving genericEvent events. The class that is interested in processing a genericEvent
   * event implements this interface, and the object created with that class is registered with a component using the
   * component's <code>addGenericEventListener<code> method. When
   * the genericEvent event occurs, that object's appropriate
   * method is invoked.
   *
   * @see GenericEventEvent
   */
  class GenericEventListener extends AsyncEventListener<LensEvent> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void process(LensEvent event) {
      processed = true;
      latch.countDown();
      LOG.info("LensEvent:" + event.getEventId());
    }
  }

  /**
   * The listener interface for receiving mockFailed events. The class that is interested in processing a mockFailed
   * event implements this interface, and the object created with that class is registered with a component using the
   * component's <code>addMockFailedListener<code> method. When
   * the mockFailed event occurs, that object's appropriate
   * method is invoked.
   *
   * @see MockFailedEvent
   */
  class MockFailedListener implements LensEventListener<QueryFailed> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(QueryFailed change) throws LensException {
      processed = true;
      latch.countDown();
      LOG.info("Query Failed event: " + change);
    }
  }

  /**
   * The listener interface for receiving mockEnded events. The class that is interested in processing a mockEnded event
   * implements this interface, and the object created with that class is registered with a component using the
   * component's <code>addMockEndedListener<code> method. When
   * the mockEnded event occurs, that object's appropriate
   * method is invoked.
   *
   * @see MockEndedEvent
   */
  class MockEndedListener implements LensEventListener<QueryEnded> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(QueryEnded change) throws LensException {
      processed = true;
      latch.countDown();
      LOG.info("Query ended event: " + change);
    }
  }

  /**
   * The Class MockQueuePositionChange.
   */
  class MockQueuePositionChange implements LensEventListener<QueuePositionChange> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(QueuePositionChange change) throws LensException {
      processed = true;
      latch.countDown();
      LOG.info("Query position changed: " + change);
    }
  }

  /**
   * Setup.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeTest
  public void setup() throws Exception {
    LensServices.get().init(LensServerConf.get());
    LensServices.get().start();
    service = LensServices.get().getService(LensEventService.NAME);
    assertNotNull(service);
    LOG.info("Service started " + service);
  }

  /**
   * Test add listener.
   */
  @Test
  public void testAddListener() {
    int listenersBefore = ((EventServiceImpl) service).getEventListeners().keySet().size();
    genericEventListener = new GenericEventListener();
    service.addListener(genericEventListener);
    endedListener = new MockEndedListener();
    service.addListener(endedListener);
    failedListener = new MockFailedListener();
    service.addListener(failedListener);
    queuePositionChangeListener = new MockQueuePositionChange();
    service.addListener(queuePositionChangeListener);

    assertTrue(service.getListeners(LensEvent.class).contains(genericEventListener));
    assertTrue(service.getListeners(QueryFailed.class).contains(failedListener));
    assertTrue(service.getListeners(QueryEnded.class).contains(endedListener));
    assertTrue(service.getListeners(QueuePositionChange.class).contains(queuePositionChangeListener));
  }

  /**
   * Test remove listener.
   */
  @Test
  public void testRemoveListener() {
    MockFailedListener toRemove = new MockFailedListener();
    service.addListener(toRemove);
    assertEquals(service.getListeners(QueryFailed.class).size(), 2);
    service.removeListener(toRemove);
    assertEquals(service.getListeners(QueryFailed.class).size(), 1);
  }

  /**
   * Reset listeners.
   */
  private void resetListeners() {
    genericEventListener.processed = false;
    endedListener.processed = false;
    failedListener.processed = false;
    queuePositionChangeListener.processed = false;
  }

  /**
   * Test handle event.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testHandleEvent() throws Exception {
    QueryHandle query = new QueryHandle(UUID.randomUUID());
    String user = "user";
    long now = System.currentTimeMillis();
    QueryFailed failed = new QueryFailed(now, QueryStatus.Status.RUNNING, QueryStatus.Status.FAILED, query, user, null);
    QuerySuccess success = new QuerySuccess(now, QueryStatus.Status.RUNNING, QueryStatus.Status.SUCCESSFUL, query);
    QueuePositionChange positionChange = new QueuePositionChange(now, 1, 0, query);

    try {
      latch = new CountDownLatch(3);
      LOG.info("Sending event: " + failed);
      service.notifyEvent(failed);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(endedListener.processed);
      assertTrue(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      latch = new CountDownLatch(2);
      LOG.info("Sending event : " + success);
      service.notifyEvent(success);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(endedListener.processed);
      assertFalse(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      latch = new CountDownLatch(2);
      LOG.info("Sending event: " + positionChange);
      service.notifyEvent(positionChange);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertFalse(endedListener.processed);
      assertFalse(failedListener.processed);
      assertTrue(queuePositionChangeListener.processed);
      resetListeners();

      LensEvent genEvent = new LensEvent(now) {
        @Override
        public String getEventId() {
          return "TEST_EVENT";
        }
      };

      latch = new CountDownLatch(1);
      LOG.info("Sending generic event " + genEvent.getEventId());
      service.notifyEvent(genEvent);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertFalse(endedListener.processed);
      assertFalse(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
    } catch (LensException e) {
      fail(e.getMessage());
    }
  }
}
