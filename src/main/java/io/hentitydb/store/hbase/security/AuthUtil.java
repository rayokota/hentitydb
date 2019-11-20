package io.hentitydb.store.hbase.security;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AuthUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AuthUtil.class);

    public static synchronized UserGroupInformation login(String principal, String keytab) {
        // resolve the requested principal, if it is present
        String finalPrincipal = null;
        if (principal != null && !principal.isEmpty()) {
            try {
                // resolves _HOST pattern using standard Hadoop search/replace
                // via DNS lookup when 2nd argument is empty
                finalPrincipal = SecurityUtil.getServerPrincipal(principal, "");
            } catch (IOException e) {
                LOG.error("Failed to resolve Kerberos principal: " + e.getMessage(), e);
                throw new SecurityException("Failed to resolve Kerberos principal", e);
            }
        }

        // check if there is a user already logged in
        UserGroupInformation currentUser = null;
        try {
            currentUser = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            // not a big deal but this shouldn't typically happen because it will
            // generally fall back to the UNIX user
            LOG.debug("Unable to get login user before Kerberos auth attempt", e);
        }

        // if the current user is valid (matches the given principal) then use it
        if (currentUser != null) {
            if (finalPrincipal == null ||
                    finalPrincipal.equals(currentUser.getUserName())) {
                LOG.debug("Using existing login for {}: {}",
                        finalPrincipal, currentUser);
                return currentUser;
            } else {
                LOG.warn("Login is replacing principal " + finalPrincipal +
                        " for " + currentUser.getUserName());
            }
        }

        // prepare for a new login
        Preconditions.checkArgument(principal != null && !principal.isEmpty(),
                "Invalid Kerberos principal: " + String.valueOf(principal));
        Preconditions.checkNotNull(finalPrincipal,
                "Resolved principal must not be null");
        Preconditions.checkArgument(keytab != null && !keytab.isEmpty(),
                "Invalid Kerberos keytab: " + String.valueOf(keytab));
        File keytabFile = new File(keytab);
        Preconditions.checkArgument(keytabFile.isFile() && keytabFile.canRead(),
                "Keytab is not a readable file: " + String.valueOf(keytab));
        try {
            // attempt static kerberos login
            LOG.debug("Logging in as {} with {}", finalPrincipal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            return UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            LOG.error("Kerberos login failed: " + e.getMessage(), e);
            throw new SecurityException("Kerberos login failed", e);
        }
    }

    public static ScheduledChore getAuthChore(final UserGroupInformation ugi) {

        Stoppable stoppable = new Stoppable() {
            private volatile boolean isStopped = false;

            @Override
            public void stop(String why) {
                isStopped = true;
            }

            @Override
            public boolean isStopped() {
                return isStopped;
            }
        };

        // if you're in debug mode this is useful to avoid getting spammed by the getTGT()
        // you can increase this, keeping in mind that the default refresh window is 0.8
        // e.g. 5min tgt * 0.8 = 4min refresh so interval is better be way less than 1min
        final int CHECK_TGT_INTERVAL = 30 * 1000; // 30sec

        ScheduledChore refreshCredentials =
                new ScheduledChore("RefreshCredentials", stoppable, CHECK_TGT_INTERVAL) {
                    @Override
                    protected void chore() {
                        try {
                            LOG.debug("Checking TGT");
                            log(ugi);
                            ugi.checkTGTAndReloginFromKeytab();
                            LOG.debug("Done checking TGT");
                        } catch (IOException e) {
                            LOG.error("Got exception while trying to refresh credentials: " + e.getMessage(), e);
                        } catch (Throwable t) {
                            LOG.error("Got throwable while trying to refresh credentials: " + t.getMessage(), t);
                        }
                    }
                };

        return refreshCredentials;
    }

    private static void log(UserGroupInformation ugi) {
        LOG.debug("UGI: " + ugi);
        LOG.debug("Auth method: " + ugi.getAuthenticationMethod());
        LOG.debug("Keytab: " + ugi.isFromKeytab());
        LOG.debug("Security enabled: " + UserGroupInformation.isSecurityEnabled());
    }
}
