"use strict";

var expect    = require("chai").expect;
var KueScheduler = require("../");

describe("schedule once", function() {
    this.timeout(60000);

    var queue = KueScheduler.createQueue();

    it('schedule with date', function (done) {
        let job_name = 'test_once_' + Date.now();

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
        });

        setTimeout(function () {
            done(new Error('timeout once'));
        }, 5000);
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
            console.log("running",job_name);
            done_job();

            counter++;

            if (counter > 1) {
                done();
            }
        });

        setTimeout(function () {
            done(new Error('timeout recurrent'));
        }, 5000);
    });

    it('schedule with recurrence handling failure', function (done) {
        let job_name = 'test_recurrence_' + Date.now();

        var job = queue.create(job_name, { some_field: 'with value' });

        queue.every('1 second',job, function (result) {
            if (result instanceof Error) {
                done(result);
            }
        });

        var counter = 0;

        queue.process(job_name, function (job, ctx, done_job) {
            console.log("running",job_name);

            if (counter == 0) {
                done_job(new Error('simulated error'));
            } else {
                done_job();
                done();
            }

            counter++;
        });

        setTimeout(function () {
            done(new Error('timeout recurrent'));
        }, 5000);
    });
});