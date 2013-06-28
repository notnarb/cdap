package com.continuuity.performance.runner;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.performance.application.BenchmarkManagerFactory;
import com.continuuity.performance.application.BenchmarkStreamWriterFactory;
import com.continuuity.performance.application.DefaultBenchmarkManager;
import com.continuuity.performance.application.MensaMetricsReporter;
import com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.test.app.ApplicationManager;
import com.continuuity.test.app.DefaultProcedureClient;
import com.continuuity.test.app.ProcedureClient;
import com.continuuity.test.app.ProcedureClientFactory;
import com.continuuity.test.app.StreamWriter;
import com.continuuity.test.app.TestHelper;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.internal.runners.model.ReflectiveCallable;
import org.junit.internal.runners.statements.Fail;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.TestClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Runner for performance tests. This class is using lots of classes and code from JUnit framework.
 */
public final class PerformanceTestRunner {

  private static final Logger LOG = LoggerFactory.getLogger(PerformanceTestRunner.class);
  private static AppFabricService.Iface appFabricServer;
  private static LocationFactory locationFactory;
  private static Injector injector;
  private static String accountId = "developer";

  private TestClass testClass;
  private final CConfiguration config;

  private PerformanceTestRunner() {
    config = CConfiguration.create();
  }

  // Parsing the command line options.
  private boolean parseOptions(String[] args) throws ClassNotFoundException {
    testClass = new TestClass(Class.forName(args[0]));

    if (args.length == 1) {
      return true;
    }

    int start;
    if (args[1].endsWith(".xml")) {
      config.addResource(args[1]);
      start = 2;
    } else {
      start = 1;
    }

    LOG.debug("Parsing command line options...");
    for (int i = start; i < args.length; i++) {
      if (i + 1 < args.length) {
        String key = args[i];
        String value = args[++i];
        if ("accountid".equalsIgnoreCase(key)) {
          accountId = value;
        } else {
          if (key.startsWith("perf.")) {
            config.set(key, value);
          } else {
            config.set("perf." + key, value);
          }

        }
      } else {
        throw new RuntimeException("<key> must have an argument <value>.");
      }
    }
    return true;
  }

  public static void main(String[] args) throws Throwable {
    PerformanceTestRunner runner = new PerformanceTestRunner();
    boolean ok = runner.parseOptions(args);
    if (ok) {
      runner.runTest();
    }
  }

  // Core method that executes all the test methods of the current performance test class.
  private void runTest() throws Throwable {
    List<Throwable> errors = new ArrayList<Throwable>();

    try {

      // execute initialization steps
      beforeClass();

      // execute all methods annotated with @BeforeClass in the current test class before running any test method.
      for (FrameworkMethod eachBefore : testClass.getAnnotatedMethods(BeforeClass.class)) {
        try {
          eachBefore.invokeExplosively(null);
        } catch (Throwable e) {
          errors.add(e);
        }
      }
      Object testClassConstructor = getTarget();

      // for each of the performance test methods annotated with @PerformanceTest execute the following sequence of
      // methods
      for (FrameworkMethod eachTest : testClass.getAnnotatedMethods(PerformanceTest.class)) {
        for (FrameworkMethod eachBefore : testClass.getAnnotatedMethods(Before.class)) {
          // execute all methods annotated with @Before right before running the current performance test method
          eachBefore.invokeExplosively(testClassConstructor);
        }
        // execute the current performance test method
        eachTest.invokeExplosively(testClassConstructor);
        for (FrameworkMethod eachAfter : testClass.getAnnotatedMethods(After.class)) {
          try {
            // execute all methods annotated with @After right after running the current performance test method
            eachAfter.invokeExplosively(testClassConstructor);
          } catch (Throwable e) {
            errors.add(e);
          }
        }
        // execute this method after running the current performance test method
        afterMethod();
      }

      // execute all methods annotated with @AfterClass after running all defined test methods
      for (FrameworkMethod eachAfter : testClass.getAnnotatedMethods(AfterClass.class)) {
        try {
          eachAfter.invokeExplosively(null);
        } catch (Throwable e) {
          errors.add(e);
        }
      }
      MultipleFailureException.assertEmpty(errors);
    } finally {
      // execute this method at the end of the overall performance test
      afterClass();
    }
  }

  // Gets methods from test class that are annotated with @annotationClass
  @SuppressWarnings("unused")
  private List<FrameworkMethod> getAnnotatedMethods(Class<? extends Annotation> annotationClass) {
    return testClass.getAnnotatedMethods(annotationClass);
  }

  // Gets all annotations of given class.
  @SuppressWarnings("unused")
  private Annotation[] getClassAnnotations() {
    return testClass.getAnnotations();
  }

  // Gets an object of TestClass by calling parameter-less constructor.
  private Object getTarget() {
    Object test;
    try {
      test = new ReflectiveCallable() {
        @Override
        protected Object runReflectiveCall() throws Throwable {
          return testClass.getOnlyConstructor().newInstance();
        }
      }.run();
      return test;
    } catch (Throwable e) {
      return new Fail(e);
    }
  }

  // Deploys a provided Continuuity Reactor App.
  public static ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    try {

      ApplicationSpecification appSpec = applicationClz.newInstance().configure();

      Location deployedJar = TestHelper.deployApplication(appFabricServer, locationFactory, new Id.Account(accountId),
                                                          TestHelper.DUMMY_AUTH_TOKEN, "", appSpec.getName(),
                                                          applicationClz);

      BenchmarkManagerFactory bmf = injector.getInstance(BenchmarkManagerFactory.class);
      ApplicationManager am = bmf.create(TestHelper.DUMMY_AUTH_TOKEN, accountId, appSpec.getName(), appFabricServer,
                                         deployedJar, appSpec);
      return am;

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Wipes out all applications and data for a given account in the Reactor.
  protected void clearAppFabric() {
    try {
      appFabricServer.reset(TestHelper.DUMMY_AUTH_TOKEN, accountId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Initializes Reactor for executing a performance test.
  private void init() {
    LOG.debug("Initializing Continuuity Reactor for a performance test.");
    File testAppDir = Files.createTempDir();

    File outputDir = new File(testAppDir, "app");
    File tmpDir = new File(testAppDir, "tmp");
    outputDir.mkdirs();
    tmpDir.mkdirs();

    config.set("app.output.dir", outputDir.getAbsolutePath());
    config.set("app.tmp.dir", tmpDir.getAbsolutePath());

    try {
      LOG.debug("Connecting with remote AppFabric server");
      appFabricServer = getAppFabricClient();
    } catch (TTransportException e) {
      LOG.error("Error when trying to open connection with remote AppFabric.");
      Throwables.propagate(e);
    }

    Module dataFabricModule;
    if (config.get("perf.datafabric.mode") != null
      && config.get("perf.datafabric.mode").equals("distributed")) {
      dataFabricModule = new DataFabricModules().getDistributedModules();
    } else {
      dataFabricModule = new DataFabricModules().getSingleNodeModules();
    }

    injector = Guice
      .createInjector(dataFabricModule,
                      new ConfigModule(config),
                      new IOModule(),
                      new LocationRuntimeModule().getInMemoryModules(),
                      new DiscoveryRuntimeModule().getInMemoryModules(),
                      new ProgramRunnerRuntimeModule().getInMemoryModules(), new AbstractModule() {
          @Override
          protected void configure() {
            install(new FactoryModuleBuilder().implement(ApplicationManager.class,
                                                         DefaultBenchmarkManager.class).build
              (BenchmarkManagerFactory.class));
            install(new FactoryModuleBuilder().implement(StreamWriter.class,
                                                         MultiThreadedStreamWriter.class).build
              (BenchmarkStreamWriterFactory.class));
            install(new FactoryModuleBuilder().implement(ProcedureClient.class,
                                                         DefaultProcedureClient.class).build
              (ProcedureClientFactory.class));
          }
        }, new Module() {
          @Override
          public void configure(Binder binder) {
            binder.bind(new TypeLiteral<PipelineFactory<?>>() {
            }).to(new TypeLiteral<SynchronousPipelineFactory<?>>() {
            });
            binder.bind(ManagerFactory.class).to(SyncManagerFactory.class);

            binder.bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
            binder.bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
            binder.bind(AppFabricService.Iface.class).toInstance(appFabricServer);
            binder.bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
            binder.bind(StoreFactory.class).to(MDSStoreFactory.class);
            binder.bind(AuthToken.class).toInstance(TestHelper.DUMMY_AUTH_TOKEN);
          }
        }
      );

    locationFactory = injector.getInstance(LocationFactory.class);
  }

  // Get an AppFabricClient for communication with the AppFabric of a local or remote Reactor.
  private static AppFabricService.Client getAppFabricClient() throws TTransportException  {
    CConfiguration config = CConfiguration.create();
    return new AppFabricService.Client(getThriftProtocol(config.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS,
                                                                    Constants.DEFAULT_APP_FABRIC_SERVER_ADDRESS),
                                                         config.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT,
                                                                       Constants.DEFAULT_APP_FABRIC_SERVER_PORT)));
  }

  private static TProtocol getThriftProtocol(String serviceHost, int servicePort) throws TTransportException {
    TTransport transport = new TFramedTransport(new TSocket(serviceHost, servicePort));
    try {
      transport.open();
    } catch (TTransportException e) {
      String message = String.format("Unable to connect to thrift service at %s:%d. Reason: %s", serviceHost,
                                     servicePort, e.getMessage());
      LOG.error(message);
      throw e;
    }
    return new TBinaryProtocol(transport);
  }

  // Gets executed once before running all the test methods of the current performance test class.
  private void beforeClass() throws ClassNotFoundException {
    // initializes Reactor
    init();
    Context.getInstance(this);

    if ("true".equalsIgnoreCase(config.get("perf.reporter.enabled"))) {
      String metrics = config.get("perf.report.metrics");
      if (StringUtils.isNotEmpty(metrics)) {
        List<String> metricList = ImmutableList.copyOf(metrics.replace(" ", "").split(","));
        String tags = "";
        int interval = 10;
        if (StringUtils.isNotEmpty(config.get("perf.report.interval"))) {
          interval = Integer.valueOf(config.get("perf.report.interval"));
        }
        Context.report(metricList, tags, interval);
      }
    }
  }

  private void afterMethod() {
    clearAppFabric();
  }

  private void afterClass() {
    Context.stopAll();
  }

  // Context for managing components of a performance test.
  private static final class Context {
    private static Context one;

    private final PerformanceTestRunner runner;
    private final Set<MultiThreadedStreamWriter> streamWriters;
    private final List<MensaMetricsReporter> mensaReporters;

    private static Context getInstance(PerformanceTestRunner runner) {
      if (one == null) {
        one = new Context(runner);
      }
      return one;
    }

    private static Context getInstance() {
      if (one == null) {
        throw new RuntimeException("PerformanceTestRunner has not instantiated Context.");
      }
      return one;
    }

    private Context(PerformanceTestRunner runner) {
      this.runner = runner;
      streamWriters = new HashSet<MultiThreadedStreamWriter>();
      mensaReporters = new ArrayList<MensaMetricsReporter>();
    }

    private final CConfiguration getConfiguration() {
      return runner.config;
    }

    /**
     * Add counters to list of metrics that get frequently collected and reported.
     * @param counters List of counter names to be frequently collected and reported.
     * @param tags Comma separated tags that will be added each time counter values are reported.
     * @param interval Time interval of collection and reporting.
     */
    public static void report(List<String> counters, String tags, int interval) {
      MensaMetricsReporter reporter =
        new MensaMetricsReporter(getInstance().getConfiguration(), counters, tags, interval);
      getInstance().mensaReporters.add(reporter);
    }

    /**
     * Immediately report counter and value.
     * @param counter Name of counter.
     * @param value Value of counter.
     */
    @SuppressWarnings("unused")
    public static void reportNow(String counter, double value) {
      if (!getInstance().mensaReporters.isEmpty()) {
        getInstance().mensaReporters.get(0).reportNow(counter, value);
      }
    }

    /**
     * Immediately collect value of counter and report it.
     * @param counter Name of counter.
     */
    @SuppressWarnings("unused")
    public static void reportNow(String counter) {
      if (!getInstance().mensaReporters.isEmpty()) {
        getInstance().mensaReporters.get(0).reportNow(counter);
      }
    }

    // Stopping all stream writers and metrics reporters
    private static void stopAll() {
      for (MultiThreadedStreamWriter streamWriter : getInstance().streamWriters) {
        streamWriter.shutdown();
      }
      for (MensaMetricsReporter reporter : getInstance().mensaReporters) {
        reporter.shutdown();
      }
    }
  }
}
