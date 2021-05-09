from dask_accelerated import helpers
import time


def warm_workers(client, scheduler, benchmark_config):

    print('Warming workers... ', end='')

    for in_size in benchmark_config['in_sizes']:

        lazy_result = helpers.get_lazy_result(
            in_size,
            benchmark_config['batch_size'],
            in_size
        )

        graph = lazy_result.__dask_graph__()

        # Scheduler does round robin
        # so we can run this for each worker in the pool
        for worker in scheduler.workers:
            # Dry run
            res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

    print('done')


def run_all_benchmarks(client, scheduler, data, benchmark_config):

    data_in_size = run_in_benchmark(client, benchmark_config)
    data_batch_size = run_batch_benchmark(client, benchmark_config)

    # Count the number of accelerated workers
    accelerated_workers = 0
    for worker in scheduler.workers:
        if str(scheduler.workers[worker].name).split('-')[0] == 'accelerated':
            accelerated_workers += 1

    data[str(accelerated_workers)] = {
        'in_size': data_in_size,
        'batch_size': data_batch_size
    }

    return data


def run_in_benchmark(client, benchmark_config):

    data_in_size = {}

    for in_size in benchmark_config['in_sizes']:

        lazy_result = helpers.get_lazy_result(
            in_size,
            benchmark_config['batch_size'],
            benchmark_config['const_batch_aggregate']
        )

        graph = lazy_result.__dask_graph__()

        # Dry run
        # res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

        data_in_size[in_size] = 0

        for i in range(benchmark_config['repeats']):
            start = time.time()
            res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))
            end = time.time()

            duration = end - start
            if i > 2:
                data_in_size[in_size] += duration
            print(
                'In: ', in_size,
                '\tBatch: ', benchmark_config['batch_size'] * benchmark_config['const_batch_aggregate'],
                '\tComputed ', res, ' in ', duration, ' seconds'
            )

        data_in_size[in_size] = data_in_size[in_size] / (benchmark_config['repeats'] - 3)

    return data_in_size


def run_batch_benchmark(client, benchmark_config):

    data_batch_size = {}

    for batch_aggregate in benchmark_config['batch_aggregates']:

        lazy_result = helpers.get_lazy_result(
            benchmark_config['const_in_size'],
            benchmark_config['batch_size'],
            batch_aggregate
        )

        graph = lazy_result.__dask_graph__()

        # Dry run
        # res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))

        data_batch_size[batch_aggregate] = 0

        for i in range(benchmark_config['repeats']):
            start = time.time()
            res = client.get(graph, (lazy_result.__dask_layers__()[0], 0))
            end = time.time()

            duration = end - start
            if i > 2:
                data_batch_size[batch_aggregate] += duration
            print(
                'In: ', benchmark_config['const_in_size'],
                '\tBatch: ', benchmark_config['batch_size'] * batch_aggregate,
                'Computed ', res, ' in ', duration, ' seconds'
            )

        data_batch_size[batch_aggregate] = data_batch_size[batch_aggregate] / (benchmark_config['repeats'] - 3)

    return data_batch_size
