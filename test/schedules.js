"use strict";

var expect = require('chai').expect;
var moment = require('moment');
var KueScheduler = require('../');

describe("schedule", function() {
    this.timeout(60000);

    var queue = KueScheduler.createQueue();

    it('schedule now', function (done) {
        let job_name = 'test_now_' + Date.now();

        var job = queue.create(job_name, { some_field: 'with value' });

        queue.now(job, function (result) {
            if (result instanceof Error) {
                done(result);
            }
        });

        queue.process(job_name, function (job, ctx, done_job) {
            console.log("running",job_name);
            done_job();
            done();

            setTimeout(job.remove,1000);
        });
    });

    it('schedule with human interval', function (done) {
        let job_name = 'test_once_human_' + Date.now();

        var job = queue.create(job_name, { some_field: 'with value' });

        queue.schedule('1 second',job, function (result) {
            if (result instanceof Error) {
                done(result);
            }
        });

        queue.process(job_name, function (job, ctx, done_job) {
            console.log("running",job_name);
            done_job();
            done();
            setTimeout(job.remove,1000);
        });
    });

    it('schedule with date', function (done) {
        let job_name = 'test_once_date_' + Date.now();

        var job = queue.create(job_name, { some_field: 'with value' });

        let scheduledDate = moment().add(1,'seconds').toDate();

        queue.schedule(scheduledDate,job, function (result) {
            if (result instanceof Error) {
                done(result);
            }
        });

        queue.process(job_name, function (job, ctx, done_job) {
            //console.log("running",job_name);
            done_job();
            done();
            setTimeout(job.remove,1000);
        });
    });

    it('schedule with recurrence', function (done) {
        let job_name = 'test_recurrence_' + Date.now();

        var job = queue.create(job_name, { some_field: 'with value' });

        queue.every('1 second',job, function (result) {
            if (result instanceof Error) {
                done(result);
            }
        });

        var counter = 0;

        queue.process(job_name, function (job, ctx, done_job) {
            //console.log("running",job_name);
            done_job();

            counter++;

            if (counter == 2) {
                done();
            }

            setTimeout(job.remove,2000);
        });
    });

    it('schedule with recurrence handling failure', function (done) {
        let job_name = 'test_recurrence_failure_' + Date.now();

        var job = queue.create(job_name, { some_field: 'with value' });

        queue.every('1 second',job, function (result) {
            if (result instanceof Error) {
                done(result);
            }
        });

        var counter = 0;

        queue.process(job_name, function (job, ctx, done_job) {
            //console.log("running",job_name);

            if (counter == 0) {
                done_job(new Error('simulated error'));
            } else {
                done_job();

                if (counter == 1) {
                    done();
                }
            }

            counter++;

            setTimeout(job.remove,2000);
        });
    });
});