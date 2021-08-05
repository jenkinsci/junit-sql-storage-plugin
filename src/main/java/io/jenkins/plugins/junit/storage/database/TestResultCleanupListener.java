package io.jenkins.plugins.junit.storage.database;

import hudson.Extension;
import hudson.model.Item;
import hudson.model.Run;
import hudson.model.listeners.ItemListener;
import hudson.model.listeners.RunListener;
import io.jenkins.plugins.junit.storage.FileJunitTestResultStorage;
import io.jenkins.plugins.junit.storage.JunitTestResultStorage;
import io.jenkins.plugins.junit.storage.TestResultImpl;

public class TestResultCleanupListener {

    @Extension
    public static class RunCleanupListener extends RunListener<Run> {
        @Override
        public void onDeleted(Run run) {
            JunitTestResultStorage junitTestResultStorage = JunitTestResultStorage.find();
            if (junitTestResultStorage instanceof FileJunitTestResultStorage) {
                return;
            }

            if (junitTestResultStorage instanceof DatabaseTestResultStorage) {
                DatabaseTestResultStorage storage = (DatabaseTestResultStorage) junitTestResultStorage;
                if (storage.isSkipCleanupRunsOnDeletion()) {
                    return;
                }
            }

            TestResultImpl testResult = junitTestResultStorage.load(run.getParent().getFullName(), run.getNumber());

            if (testResult instanceof DatabaseTestResultStorage.TestResultStorage) {
                DatabaseTestResultStorage.TestResultStorage storage = (DatabaseTestResultStorage.TestResultStorage) testResult;

                storage.deleteRun();
            }
        }
    }

    @Extension
    public static class JobCleanupListener extends ItemListener {
        @Override
        public void onDeleted(Item item) {
            JunitTestResultStorage junitTestResultStorage = JunitTestResultStorage.find();
            if (junitTestResultStorage instanceof FileJunitTestResultStorage) {
                return;
            }

            if (junitTestResultStorage instanceof DatabaseTestResultStorage) {
                DatabaseTestResultStorage storage = (DatabaseTestResultStorage) junitTestResultStorage;
                if (storage.isSkipCleanupRunsOnDeletion()) {
                    return;
                }
            }

            TestResultImpl testResult = junitTestResultStorage.load(item.getFullName(), 0);

            if (testResult instanceof DatabaseTestResultStorage.TestResultStorage) {
                DatabaseTestResultStorage.TestResultStorage storage = (DatabaseTestResultStorage.TestResultStorage) testResult;
                storage.deleteJob();
            }
        }
    }
}
