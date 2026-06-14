package rx;

import rx.schedulers.IOThreadScheduler;
import rx.schedulers.SingleThreadScheduler;

public class DemoMain {

    public static void main(String[] args) {

        IOThreadScheduler ioScheduler = new IOThreadScheduler();
        SingleThreadScheduler observeScheduler = new SingleThreadScheduler();

        Observable<Integer> observable =
                Observable.create(emitter -> {
                    for (int i = 1; i <= 5; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                });

        observable
                .map(x -> x * 10)
                .filter(x -> x >= 20)
                .subscribeOn(ioScheduler)
                .observeOn(observeScheduler)
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                        System.out.println("Получено: " + item +
                                " | Поток: " + Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println("Ошибка: " + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Поток завершен");
                    }
                });

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ioScheduler.shutdown();
        observeScheduler.shutdown();
    }
}