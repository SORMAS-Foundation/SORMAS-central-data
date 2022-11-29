node {
        
    stage('checkout') {
        git branch: '${BRANCH}', url: 'https://github.com/hzi-braunschweig/SORMAS-central-data.git'
    }

    stage('reading from a file') {
        script {
            def props = readProperties file: '.env'
            env.VERSION = props.VERSION
        }
    }
    stage('Build container') {
    	echo 'Building align-local-central'
    	sh """
    	sudo buildah bud --pull-always --no-cache -t central-aligner:${VERSION} .
    	"""
    	echo 'Building infra-cleaner'
    	sh """
    	sudo buildah bud --pull-always --no-cache -f Dockerfile-Infra-Cleaner -t infra-cleaner:${VERSION} .
    	"""
    	echo 'Building central-verifier'
    	sh """
    	sudo buildah bud --pull-always --no-cache -f Dockerfile-Central-Verifier -t central-verifier:${VERSION} .
    	"""
    	echo 'Building insert-missing-name-dups'
    	sh """
    	sudo buildah bud --pull-always --no-cache -f Dockerfile-Insert-Missing-Name-Dups -t insert-missing-name-dups:${VERSION} .
    	"""  		
    }
      
    stage('Push image into registry') {
    echo 'Deploying....'
        withCredentials([ usernamePassword(credentialsId: 'registry.netzlink.com', usernameVariable: 'MY_SECRET_USER_NLI', passwordVariable: 'MY_SECRET_USER_PASSWORD_NLI' )]) {
        	sh """
        	sudo buildah login -u '$MY_SECRET_USER_NLI' -p '$MY_SECRET_USER_PASSWORD_NLI' registry.netzlink.com
        	sudo buildah push -f v2s2 central-aligner:${VERSION} registry.netzlink.com/hzibraunschweig/central-aligner:${VERSION}
            sudo buildah push -f v2s2 central-aligner:${VERSION} registry.netzlink.com/hzibraunschweig/central-aligner:latest
			sudo buildah push -f v2s2 central-aligner:${VERSION} registry.netzlink.com/hzibraunschweig/central-aligner:stable

        	sudo buildah push -f v2s2 infra-cleaner:${VERSION} registry.netzlink.com/hzibraunschweig/infra-cleaner:${VERSION}
            sudo buildah push -f v2s2 infra-cleaner:${VERSION} registry.netzlink.com/hzibraunschweig/infra-cleaner:latest
			sudo buildah push -f v2s2 infra-cleaner:${VERSION} registry.netzlink.com/hzibraunschweig/infra-cleaner:stable

        	sudo buildah push -f v2s2 central-verifier:${VERSION} registry.netzlink.com/hzibraunschweig/central-verifier:${VERSION}
            sudo buildah push -f v2s2 central-verifier:${VERSION} registry.netzlink.com/hzibraunschweig/central-verifier:latest
			sudo buildah push -f v2s2 central-verifier:${VERSION} registry.netzlink.com/hzibraunschweig/central-verifier:stable

        	sudo buildah push -f v2s2 insert-missing-name-dups:${VERSION} registry.netzlink.com/hzibraunschweig/insert-missing-name-dups:${VERSION}
            sudo buildah push -f v2s2 insert-missing-name-dups:${VERSION} registry.netzlink.com/hzibraunschweig/insert-missing-name-dups:latest
			sudo buildah push -f v2s2 insert-missing-name-dups:${VERSION} registry.netzlink.com/hzibraunschweig/insert-missing-name-dups:stable			
        	"""
        }    
	}
}
