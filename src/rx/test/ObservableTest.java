package rx.test;

import org.junit.jupiter.api.Test;
import rx.Disposable;
import rx.Observable;
import rx.Observer;
import rx.schedulers.ComputationScheduler;
import rx.schedulers.IOThreadScheduler;
import rx.schedulers.SingleThreadScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ObservableTest {

    @Test
    void testMapFilter() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> observable =
                Observable.create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                });

        observable
                .map(x -> x * 2)
                .filter(x -> x > 2)
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Ошибки быть не должно");
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertEquals(List.of(4, 6), result);
    }

    @Test
    void testFlatMap() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source =
                Observable.create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                });

        Observable<Integer> mapped = source.flatMap(x ->
                Observable.<Integer>create(emitter -> {
                    emitter.onNext(x);
                    emitter.onNext(x * 10);
                    emitter.onComplete();
                })
        );

        mapped.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                result.add(item);
            }

            @Override
            public void onError(Throwable t) {
                fail("Ошибки быть не должно");
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals(List.of(1, 10, 2, 20), result);
    }

    @Test
    void testOnError() {
        List<String> events = new ArrayList<>();

        Observable<Integer> source =
                Observable.create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(new RuntimeException("Тестовая ошибка"));
                });

        source.subscribe(new Observer<>() {
            @Override
            public void onNext(Integer item) {
                events.add("onNext:" + item);
            }

            @Override
            public void onError(Throwable t) {
                events.add("onError:" + t.getMessage());
            }

            @Override
            public void onComplete() {
                events.add("onComplete");
            }
        });

        assertTrue(events.contains("onNext:1"));
        assertTrue(events.contains("onError:Тестовая ошибка"));
        assertFalse(events.contains("onComplete"));
    }

    @Test
    void testDisposableStateChanges() {
        Observable<Integer> source =
                Observable.create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                });

        Disposable disposable = source.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                fail("Ошибки быть не должно");
            }

            @Override
            public void onComplete() {
            }
        });

        assertNotNull(disposable);
        assertFalse(disposable.isDisposed());

        disposable.dispose();

        assertTrue(disposable.isDisposed());
    }

    @Test
    void testSubscribeOnRunsInAnotherThread() throws InterruptedException {
        IOThreadScheduler scheduler = new IOThreadScheduler();
        CountDownLatch latch = new CountDownLatch(1);

        String mainThreadName = Thread.currentThread().getName();
        String[] workerThreadName = new String[1];

        Observable<Integer> source =
                Observable.create(emitter -> {
                    workerThreadName[0] = Thread.currentThread().getName();
                    emitter.onNext(1);
                    emitter.onComplete();
                });

        source
                .subscribeOn(scheduler)
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                        fail("Ошибки быть не должно");
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        boolean completed = latch.await(3, TimeUnit.SECONDS);
        scheduler.shutdown();

        assertTrue(completed, "Подписка не завершилась вовремя");
        assertNotNull(workerThreadName[0]);
        assertNotEquals(mainThreadName, workerThreadName[0]);
    }

    @Test
    void testObserveOnRunsObserverInSingleThreadScheduler() throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();
        CountDownLatch latch = new CountDownLatch(1);

        String mainThreadName = Thread.currentThread().getName();
        String[] observerThreadName = new String[1];

        Observable<Integer> source =
                Observable.create(emitter -> {
                    emitter.onNext(100);
                    emitter.onComplete();
                });

        source
                .observeOn(scheduler)
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                        observerThreadName[0] = Thread.currentThread().getName();
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                        fail("Ошибки быть не должно");
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        boolean completed = latch.await(3, TimeUnit.SECONDS);
        scheduler.shutdown();

        assertTrue(completed, "Обработка не завершилась вовремя");
        assertNotNull(observerThreadName[0]);
        assertNotEquals(mainThreadName, observerThreadName[0]);
    }

    @Test
    void testComputationSchedulerExecutesTask() throws InterruptedException {
        ComputationScheduler scheduler = new ComputationScheduler();

        CountDownLatch latch = new CountDownLatch(1);
        String[] threadName = new String[1];

        scheduler.execute(() -> {
            threadName[0] = Thread.currentThread().getName();
            latch.countDown();
        });

        boolean completed = latch.await(2, TimeUnit.SECONDS);
        scheduler.shutdown();

        assertTrue(completed, "Задача не была выполнена");
        assertNotNull(threadName[0]);
    }
}