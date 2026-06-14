package rx;

import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    public interface OnSubscribe<T> {
        void subscribe(Emitter<T> emitter);
    }

    private final OnSubscribe<T> source;

    private Observable(OnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(OnSubscribe<T> source) {
        return new Observable<>(source);
    }

    public Disposable subscribe(Observer<T> observer) {

        SimpleDisposable disposable = new SimpleDisposable();

        source.subscribe(new Emitter<>() {
            @Override
            public void onNext(T item) {
                if (!disposable.isDisposed()) {
                    observer.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!disposable.isDisposed()) {
                    observer.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (!disposable.isDisposed()) {
                    observer.onComplete();
                }
            }
        });

        return disposable;
    }

    public <R> Observable<R> map(Function<T, R> mapper) {

        return create(emitter ->
                subscribe(new Observer<>() {
                    @Override
                    public void onNext(T item) {
                        emitter.onNext(mapper.apply(item));
                    }

                    @Override
                    public void onError(Throwable t) {
                        emitter.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                })
        );
    }

    public Observable<T> filter(Predicate<T> predicate) {

        return create(emitter ->
                subscribe(new Observer<>() {
                    @Override
                    public void onNext(T item) {
                        if (predicate.test(item)) {
                            emitter.onNext(item);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        emitter.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                })
        );
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {

        return create(emitter ->
                subscribe(new Observer<>() {
                    @Override
                    public void onNext(T item) {
                        Observable<R> inner = mapper.apply(item);
                        inner.subscribe(new Observer<>() {
                            @Override
                            public void onNext(R value) {
                                emitter.onNext(value);
                            }

                            @Override
                            public void onError(Throwable t) {
                                emitter.onError(t);
                            }

                            @Override
                            public void onComplete() {
                            }
                        });
                    }

                    @Override
                    public void onError(Throwable t) {
                        emitter.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }
                })
        );
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {

        return create(emitter ->
                scheduler.execute(() ->
                        Observable.this.subscribe(new Observer<>() {
                            @Override
                            public void onNext(T item) {
                                emitter.onNext(item);
                            }

                            @Override
                            public void onError(Throwable t) {
                                emitter.onError(t);
                            }

                            @Override
                            public void onComplete() {
                                emitter.onComplete();
                            }
                        })
                )
        );
    }

    public Observable<T> observeOn(Scheduler scheduler) {

        return create(emitter ->
                subscribe(new Observer<>() {
                    @Override
                    public void onNext(T item) {
                        scheduler.execute(() -> emitter.onNext(item));
                    }

                    @Override
                    public void onError(Throwable t) {
                        scheduler.execute(() -> emitter.onError(t));
                    }

                    @Override
                    public void onComplete() {
                        scheduler.execute(emitter::onComplete);
                    }
                })
        );
    }

    static class SimpleDisposable implements Disposable {

        private volatile boolean disposed;

        @Override
        public void dispose() {
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }
}