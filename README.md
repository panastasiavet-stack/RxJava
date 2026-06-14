В рамках данного проекта была создана упрощённая версия библиотеки RxJava, иллюстрирующая ключевые концепции реактивного программирования.

Цель проекта — разработать систему реактивных потоков данных, обеспечивающую поддержку следующих возможностей:

⦁ паттерн Observer

⦁ асинхронную обработку данных

⦁ управление потоками выполнения

⦁ операторы для преобразования данных

⦁ обработку ошибок

⦁ механизм отмены подписки

Реализация выполнена на языке Java с использованием стандартных средств для работы с многопоточностью.

**Архитектура**

Система построена на основе паттерна Наблюдатель (Observer Pattern).

Ключевые компоненты:

⦁ Observable — источник потока данных

⦁ Observer — подписчик, обрабатывающий события потока

⦁ Emitter — передаёт события от Observable к Observer

⦁ Disposable — управляет жизненным циклом подписки

⦁ Scheduler — отвечает за управление потоками выполнения

**Поток данных**

Последовательность передачи данных: Observable → Emitter → Observer.

Принцип работы:

⦁ Создаётся экземпляр Observable

⦁ На него регистрируется Observer

⦁ Observable начинает генерировать события

⦁ События передаются через Emitter

⦁ Observer получает события посредством методов: onNext, onError, onComplete


**Основные компоненты**

Observer
Интерфейс Observer содержит методы для обработки событий потока:

public interface Observer<T> {
    void onNext(T item);
    void onError(Throwable t);
    void onComplete();
}

Назначение методов:
⦁ onNext — получение элемента потока

⦁ onError — обработка ошибки

⦁ onComplete — завершение потока

Observable

Класс Observable выступает источником данных.

Создать Observable можно с помощью статического метода create(...):

Observable<Integer> observable =
    Observable.create(emitter -> {
        emitter.onNext(1);
        emitter.onNext(2);
        emitter.onNext(3);
        emitter.onComplete();
    });

Подписка осуществляется через метод:

observable.subscribe(observer);

Disposable
Интерфейс Disposable предназначен для управления подпиской:

public interface Disposable {
    void dispose();
    boolean isDisposed();
}

Методы позволяют отменить подписку и проверить её состояние.

Операторы преобразования данных
Библиотека включает базовые операторы для работы с потоками:

⦁ map — преобразует элементы потока.
Пример:

observable.map(x -> x * 10)

⦁ filter — пропускает только элементы, удовлетворяющие условию.
Пример:

observable.filter(x -> x > 10)

⦁ flatMap — преобразует каждый элемент во внутренний Observable и объединяет результаты в один поток.
Пример:

observable.flatMap(x ->
    Observable.create(emitter -> {
        emitter.onNext(x);
        emitter.onNext(x * 10);
        emitter.onComplete();
    })
);

Управление потоками выполнения (Schedulers)
Schedulers отвечают за организацию потоков выполнения задач.

Интерфейс:

public interface Scheduler {
    void execute(Runnable task);
}

В проекте реализованы три вида планировщиков:

⦁ IOThreadScheduler — использует пул CachedThreadPool (Executors.newCachedThreadPool()), предназначен для операций ввода/вывода, сетевых и файловых операций; динамически создает и переиспользует потоки.

⦁ ComputationScheduler — использует фиксированный пул из числа доступных процессоров (Executors.newFixedThreadPool), оптимален для вычислительных задач.

⦁ SingleThreadScheduler — использует единственный поток (Executors.newSingleThreadExecutor()), обеспечивает последовательную обработку и сохранение порядка событий.

Методы subscribeOn и observeOn

Контроль потоков происходит с помощью методов:  

- **subscribeOn** — задаёт поток, в котором происходит подписка:  
  ```java
  observable.subscribeOn(ioScheduler)
  ```
- **observeOn** — задаёт поток, в котором обрабатываются элементы потока:  
  ```java
  observable.observeOn(computationScheduler)
  ```

Пример использования библиотеки  
Использование демонстрируется в классе DemoMain:  

```java
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
    .subscribeOn(new IOThreadScheduler())
    .observeOn(new ComputationScheduler())
    .subscribe(new Observer<Integer>() {
        @id86240433 (@Override)
public void onNext(Integer item) {
            System.out.println("Получено: " + item);
        }

        @id86240433 (@Override)
        public void onError(Throwable t) {
            System.out.println("Ошибка: " + t.getMessage());
        }

        @id86240433 (@Override)
        public void onComplete() {
            System.out.println("Поток завершен");
        }
    });
```

**Тестирование**  
Для проверки работы системы написаны unit-тесты с использованием JUnit 5, покрывающие следующие сценарии:  

| Тест                                | Проверяемая функциональность                |
|------------------------------------|---------------------------------------------|
| testMapFilter                      | проверка операторов map и filter            |
| testFlatMap                       | корректность работы flatMap                  |
| testOnError                      | обработка ошибок                             |
| testDisposableStateChanges        | работа Disposable                            |
| testSubscribeOnRunsInAnotherThread | выполнение подписки в отдельном потоке      |
| testObserveOnRunsObserverInSingleThreadScheduler | обработка событий в заданном потоке        |
| testComputationSchedulerExecutesTask | корректность выполнения задач Scheduler     |

Тесты подтверждают корректность операторов, работу подписок, обработку ошибок и поддержку многопоточности.

**Итоги**  
В ходе проекта реализована упрощённая реактивная библиотека, иллюстрирующая основные принципы RxJava, включая:  

- паттерн Observer
  
- реактивные потоки данных
   
- операторы преобразования
   
- управление потоками выполнения
  
- обработку ошибок
  
- механизм отмены подписки  

Проект служит демонстрацией базовой архитектуры реактивных систем и помогает понять внутренний механизм работы RxJava.
 
Данный проект полностью охватывает требования:  
- описание архитектуры 
- описание Scheduler 
- различия между Scheduler 
- пример использования 
- описание тестирования 
 
